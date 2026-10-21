import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, ArrayType, MapType)

spark = SparkSession.builder.getOrCreate()

BRONZE = "/home/u2250055/project_weatherstack/bronze"
SILVER = "hdfs://hadoop-master:39000/2250055/silver"


HORA = StructType([
    StructField("precip", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("windgust", DoubleType()),
])
ASTRO = StructType([
    StructField("sunrise", StringType()),
    StructField("sunset", StringType()),
])
DIA = StructType([
    StructField("mintemp", DoubleType()),
    StructField("maxtemp", DoubleType()),
    StructField("avgtemp", DoubleType()),
    StructField("sunhour", DoubleType()),
    StructField("uv_index", DoubleType()),
    StructField("totalsnow", DoubleType()),
    StructField("astro", ASTRO),
    StructField("hourly", ArrayType(HORA)),
])


def meses_no_bronze(bronze):
    pares = []
    for ano_dir in sorted(os.listdir(bronze)):
        if not ano_dir.startswith("ano="):
            continue
        ano = ano_dir.split("=")[1]
        for mes_dir in sorted(os.listdir(os.path.join(bronze, ano_dir))):
            if not mes_dir.startswith("mes="):
                continue
            pares.append((ano, mes_dir.split("=")[1]))
    return pares


partes = []

for ano, mes in meses_no_bronze(BRONZE):
    print(f"[SILVER] {ano}-{mes}")

    path = f"{BRONZE}/ano={ano}/mes={mes}/distrito=*/*.json"
    df = spark.read.json(path).withColumn("_src", F.input_file_name())

    # 'historical' (struct com datas por campo) -> map(date -> DIA), com schema fixo.
    df = df.withColumn(
        "hist_map",
        F.from_json(F.to_json("historical"), MapType(StringType(), DIA)),
    )

    dias = df.select(
        F.regexp_replace(F.regexp_extract(F.col("_src"), r"distrito=([^/]+)/", 1), "%20", " ").alias("district"),
        F.col("location.country").alias("country"),
        F.col("location.lat").cast("double").alias("lat"),
        F.col("location.lon").cast("double").alias("lon"),
        F.explode("hist_map").alias("date_str", "dia"),
    )

    diario = dias.select(
        "district", "country", "lat", "lon",
        F.to_date("date_str", "yyyy-MM-dd").alias("date"),
        F.col("dia.astro.sunrise").alias("sunrise"),
        F.col("dia.astro.sunset").alias("sunset"),
        F.col("dia.mintemp").alias("mintemp"),
        F.col("dia.maxtemp").alias("maxtemp"),
        F.col("dia.avgtemp").alias("avgtemp"),
        F.col("dia.sunhour").alias("sunhour"),
        F.col("dia.uv_index").alias("uv_index"),
        F.col("dia.totalsnow").alias("totalsnow"),
        F.col("dia.hourly").alias("hourly"),
    )

    horas = diario.select(
        "district", "date", F.explode("hourly").alias("h")
    ).select(
        "district", "date",
        F.col("h.precip").alias("precip"),
        F.col("h.humidity").alias("humidity"),
        F.col("h.wind_speed").alias("wind_speed"),
        F.col("h.windgust").alias("windgust"),
    )

    agg = horas.groupBy("district", "date").agg(
        F.round(F.sum("precip"), 2).alias("precip_total"),        # <- SOMATÓRIO da precipitação
        F.round(F.avg("humidity"), 1).alias("humidity_media"),    # <- MÉDIA da humidade
        F.max("humidity").alias("humidity_max"),                  # <- MÁXIMO da humidade
        F.min("humidity").alias("humidity_min"),                  # <- mínimo da humidade
        F.round(F.avg("wind_speed"), 1).alias("wind_media"),      # <- MÉDIA do vento
        F.max("wind_speed").alias("wind_max"),                    # <- MÁXIMO do vento
        F.max("windgust").alias("rajada_max"),                    # <- MÁXIMO da rajada
    )

    silver_mes = (
        diario.drop("hourly")
        .join(agg, ["district", "date"], "left")
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    )
    partes.append(silver_mes)

silver = partes[0]
for p in partes[1:]:
    silver = silver.unionByName(p)

silver = silver.orderBy("district", "date")
silver.coalesce(1).write.mode("overwrite").partitionBy("year_month").parquet(SILVER)

print(f"\nSilver criado em {SILVER}")
print(f"Linhas: {silver.count()} | Distritos: {silver.select('district').distinct().count()}")
print(f"Dias com precip nula: {silver.filter(F.col('precip_total').isNull()).count()}")
