# =============================================================================
# user_update.py
# Atualização INCREMENTAL do pipeline weatherstack (bronze -> silver -> gold),
# já a escrever no HDFS. Vai buscar só os dias em falta (até ontem), reprocessa
# apenas os meses afetados e grava com PARTITION OVERWRITE DINÂMICO, por isso
# NUNCA toca no histórico já existente.
#
# Pré-requisito: o backfill inicial (bronze/silver/gold) já tem de estar no HDFS.
# =============================================================================

import os
import shutil
import calendar
import subprocess
from datetime import date, timedelta

import requests
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, ArrayType, MapType)

spark = SparkSession.builder.getOrCreate()
# CRÍTICO: só substitui as partições (meses) que escrevemos; o resto fica intacto.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
API_KEY = "6fc53d10d52e414f5b9bd3fc669bca5c"

DISTRITOS = [
    "Aveiro", "Beja", "Braga", "Braganca", "Castelo Branco", "Coimbra",
    "Evora", "Faro", "Guarda", "Leiria", "Lisboa", "Portalegre",
    "Porto", "Santarem", "Setubal", "Viana do Castelo", "Vila Real", "Viseu",
]

# Staging local (a extração escreve aqui antes de subir ao HDFS)
LOCAL_BRONZE = "/home/u2250055/project_weatherstack/bronze"

# Destinos no HDFS
HDFS_BRONZE       = "hdfs://hadoop-master:39000/2250055/bronze"
HDFS_SILVER       = "hdfs://hadoop-master:39000/2250055/silver"
HDFS_GOLD_DAILY   = "hdfs://hadoop-master:39000/2250055/gold/gold_daily"
HDFS_GOLD_MONTHLY = "hdfs://hadoop-master:39000/2250055/gold/gold_monthly"

HOURLY = 1
INTERVAL = 1
LIMIAR_CHUVA_INTENSA = 10.0
BASE_GRAUS_DIA = 18.0

# ----------------------------------------------------------------------------
# Schemas explícitos (numéricos em DOUBLE para não perder decimais — ex.: chuva)
# ----------------------------------------------------------------------------
HORA = StructType([
    StructField("precip", DoubleType()), StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()), StructField("windgust", DoubleType()),
])
ASTRO = StructType([StructField("sunrise", StringType()), StructField("sunset", StringType())])
DIA = StructType([
    StructField("mintemp", DoubleType()), StructField("maxtemp", DoubleType()),
    StructField("avgtemp", DoubleType()), StructField("sunhour", DoubleType()),
    StructField("uv_index", DoubleType()), StructField("totalsnow", DoubleType()),
    StructField("astro", ASTRO), StructField("hourly", ArrayType(HORA)),
])

# ----------------------------------------------------------------------------
# Auxiliares de transformação (iguais aos do generate_silver / generate_gold)
# ----------------------------------------------------------------------------
def to_min(col):
    ts = F.to_timestamp(col, "hh:mm a")
    return F.hour(ts) * 60 + F.minute(ts)

def heat_index(tc, rh):
    tf = tc * 9.0 / 5.0 + 32.0
    hi_f = (-42.379 + 2.04901523*tf + 10.14333127*rh - 0.22475541*tf*rh
            - 0.00683783*tf*tf - 0.05481717*rh*rh + 0.00122874*tf*tf*rh
            + 0.00085282*tf*rh*rh - 0.00000199*tf*tf*rh*rh)
    hi_c = (hi_f - 32.0) * 5.0 / 9.0
    return F.when(tc < 27, tc).otherwise(F.round(hi_c, 1))

def wind_chill(tc, v):
    vp = F.pow(v, F.lit(0.16))
    wc = 13.12 + 0.6215*tc - 11.37*vp + 0.3965*tc*vp
    return F.when((tc <= 10) & (v > 4.8), F.round(wc, 1)).otherwise(tc)

def maior_sequencia(df, flag_col, alias):
    w_all  = Window.partitionBy("district", "year_month").orderBy("date")
    w_flag = Window.partitionBy("district", "year_month", flag_col).orderBy("date")
    return (df.withColumn("_rn1", F.row_number().over(w_all))
              .withColumn("_rn2", F.row_number().over(w_flag))
              .withColumn("_grp", F.col("_rn1") - F.col("_rn2"))
              .filter(F.col(flag_col) == 1)
              .groupBy("district", "year_month", "_grp").agg(F.count("*").alias("_len"))
              .groupBy("district", "year_month").agg(F.max("_len").alias(alias)))

# ----------------------------------------------------------------------------
# 1) BRONZE: extração de um mês (todos os distritos) para staging local
# ----------------------------------------------------------------------------
def extrair_mes_bronze(ano, mes, end_day):
    ano_s, mes_s = f"{ano:04d}", f"{mes:02d}"
    for dist in DISTRITOS:
        folder = os.path.join(LOCAL_BRONZE, f"ano={ano_s}", f"mes={mes_s}", f"distrito={dist}")
        os.makedirs(folder, exist_ok=True)
        params = {
            "access_key": API_KEY, "query": f"{dist},Portugal",
            "historical_date_start": f"{ano_s}-{mes_s}-01",
            "historical_date_end": f"{ano_s}-{mes_s}-{end_day:02d}",
            "hourly": HOURLY, "interval": INTERVAL,
        }
        r = requests.get("https://api.weatherstack.com/historical", params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        if not data.get("success", True) or "historical" not in data:
            raise RuntimeError(f"Resposta inválida {dist} {ano_s}-{mes_s}: {data.get('error', data)}")
        with open(os.path.join(folder, f"{dist}_{ano_s}-{mes_s}.json"), "w", encoding="utf-8") as f:
            f.write(r.text)

def put_mes_hdfs(ano, mes):
    """Substitui no HDFS a pasta do mês (remove e volta a pôr)."""
    ano_s, mes_s = f"{ano:04d}", f"{mes:02d}"
    local_dir = os.path.join(LOCAL_BRONZE, f"ano={ano_s}", f"mes={mes_s}")
    hdfs_ano  = f"{HDFS_BRONZE}/ano={ano_s}"
    hdfs_mes  = f"{hdfs_ano}/mes={mes_s}"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_ano], check=True)
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_mes])          # limpar mês antigo
    subprocess.run(["hdfs", "dfs", "-put", local_dir, hdfs_ano + "/"], check=True)  # repor

# ----------------------------------------------------------------------------
# 2) SILVER: construir o silver de um mês a partir do bronze (no HDFS)
# ----------------------------------------------------------------------------
def silver_de_bronze(bronze_glob):
    df = spark.read.json(bronze_glob).withColumn("_src", F.input_file_name())
    df = df.withColumn("hist_map", F.from_json(F.to_json("historical"), MapType(StringType(), DIA)))

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
        F.col("dia.mintemp").alias("mintemp"), F.col("dia.maxtemp").alias("maxtemp"),
        F.col("dia.avgtemp").alias("avgtemp"), F.col("dia.sunhour").alias("sunhour"),
        F.col("dia.uv_index").alias("uv_index"), F.col("dia.totalsnow").alias("totalsnow"),
        F.col("dia.hourly").alias("hourly"),
    )
    horas = diario.select("district", "date", F.explode("hourly").alias("h")).select(
        "district", "date",
        F.col("h.precip").alias("precip"), F.col("h.humidity").alias("humidity"),
        F.col("h.wind_speed").alias("wind_speed"), F.col("h.windgust").alias("windgust"),
    )
    agg = horas.groupBy("district", "date").agg(
        F.round(F.sum("precip"), 2).alias("precip_total"),
        F.round(F.avg("humidity"), 1).alias("humidity_media"),
        F.max("humidity").alias("humidity_max"), F.min("humidity").alias("humidity_min"),
        F.round(F.avg("wind_speed"), 1).alias("wind_media"),
        F.max("wind_speed").alias("wind_max"), F.max("windgust").alias("rajada_max"),
    )
    return (diario.drop("hourly")
            .join(agg, ["district", "date"], "left")
            .withColumn("year_month", F.date_format("date", "yyyy-MM")))

# ----------------------------------------------------------------------------
# 3) GOLD: daily e monthly a partir do silver
# ----------------------------------------------------------------------------
def build_daily(silver_df):
    daily = silver_df.select(
        "district", "country", "lat", "lon", "date",
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.col("maxtemp").alias("temp_max"), F.col("mintemp").alias("temp_min"),
        F.col("avgtemp").alias("temp_media"),
        F.col("humidity_media").alias("humidade_media"),
        F.col("humidity_max").alias("humidade_max"),
        F.col("humidity_min").alias("humidade_min"),
        F.col("wind_media").alias("vento_media"), F.col("wind_max").alias("vento_max"),
        F.col("rajada_max"), F.col("uv_index"), F.col("sunhour"), F.col("precip_total"),
        F.round(F.col("maxtemp") - F.col("mintemp"), 1).alias("amplitude_termica"),
        F.round(F.col("humidity_max") - F.col("humidity_min"), 1).alias("amplitude_humidade"),
        F.round((to_min("sunset") - to_min("sunrise")) / 60.0, 2).alias("duracao_dia"),
        heat_index(F.col("maxtemp"), F.col("humidity_media")).alias("heat_index"),
        wind_chill(F.col("mintemp"), F.col("wind_media")).alias("wind_chill"),
        F.when(F.col("uv_index") <= 2, "baixo").when(F.col("uv_index") <= 5, "moderado")
         .when(F.col("uv_index") <= 7, "alto").when(F.col("uv_index") <= 10, "muito_alto")
         .otherwise("extremo").alias("categoria_uv"),
        F.when(F.col("wind_media") <= 20, "fraco").when(F.col("wind_media") <= 50, "moderado")
         .when(F.col("wind_media") <= 70, "forte").otherwise("muito_forte").alias("categoria_vento"),
        (F.col("maxtemp") > 30).cast("int").alias("dia_quente"),
        (F.col("mintemp") < 5).cast("int").alias("dia_frio"),
        (F.col("totalsnow") > 0).cast("int").alias("dia_neve"),
        (F.col("precip_total") > 0).cast("int").alias("dia_chuva"),
        (F.col("precip_total") > LIMIAR_CHUVA_INTENSA).cast("int").alias("dia_chuva_intensa"),
    )
    return daily.withColumn(
        "aproveitamento_solar",
        F.when(F.col("duracao_dia") > 0, F.round(F.col("sunhour") / F.col("duracao_dia"), 2)))

def build_monthly(daily):
    base = daily.groupBy("district", "year_month").agg(
        F.first("country").alias("country"), F.first("lat").alias("lat"), F.first("lon").alias("lon"),
        F.round(F.max("temp_max"), 1).alias("temp_max_mes"),
        F.round(F.min("temp_min"), 1).alias("temp_min_mes"),
        F.round(F.avg("temp_media"), 1).alias("temp_media_mes"),
        F.sum("dia_quente").alias("dias_quentes"), F.sum("dia_frio").alias("dias_frios"),
        F.round(F.avg("amplitude_termica"), 1).alias("amplitude_media_dia"),
        F.round(F.stddev("temp_media"), 2).alias("temp_desvio"),
        F.round(F.sum(F.greatest(F.lit(0.0), F.lit(BASE_GRAUS_DIA) - F.col("temp_media"))), 1).alias("graus_dia_aquecimento"),
        F.round(F.sum(F.greatest(F.lit(0.0), F.col("temp_media") - F.lit(BASE_GRAUS_DIA))), 1).alias("graus_dia_arrefecimento"),
        F.round(F.sum("precip_total"), 1).alias("precip_total_mes"),
        F.sum("dia_chuva").alias("dias_chuva"), F.sum("dia_chuva_intensa").alias("dias_chuva_intensa"),
        F.round(F.max("precip_total"), 1).alias("precip_max_dia_mes"),
        F.round(F.avg("humidade_media"), 1).alias("humidade_media_mes"),
        F.round(F.max("humidade_max"), 1).alias("humidade_max_mes"),
        F.round(F.min("humidade_min"), 1).alias("humidade_min_mes"),
        F.sum(F.when(F.col("humidade_media") > 80, 1).otherwise(0)).alias("dias_humidos"),
        F.sum(F.when(F.col("humidade_media") < 30, 1).otherwise(0)).alias("dias_secos"),
        F.round(F.avg("amplitude_humidade"), 1).alias("amplitude_humidade_media"),
        F.round(F.avg("vento_media"), 1).alias("vento_media_mes"),
        F.round(F.max("vento_max"), 1).alias("vento_max_mes"),
        F.round(F.max("rajada_max"), 1).alias("rajada_max_mes"),
        F.sum(F.when(F.col("vento_media") > 50, 1).otherwise(0)).alias("dias_vento_forte"),
        F.sum(F.when(F.col("categoria_vento") == "fraco", 1).otherwise(0)).alias("vento_cat_fraco"),
        F.sum(F.when(F.col("categoria_vento") == "moderado", 1).otherwise(0)).alias("vento_cat_moderado"),
        F.sum(F.when(F.col("categoria_vento") == "forte", 1).otherwise(0)).alias("vento_cat_forte"),
        F.sum(F.when(F.col("categoria_vento") == "muito_forte", 1).otherwise(0)).alias("vento_cat_muito_forte"),
        F.round(F.max("uv_index"), 1).alias("uv_max_mes"), F.round(F.avg("uv_index"), 1).alias("uv_media_mes"),
        F.sum(F.when(F.col("uv_index") >= 6, 1).otherwise(0)).alias("dias_uv_alto"),
        F.sum(F.when(F.col("uv_index") >= 8, 1).otherwise(0)).alias("dias_uv_extremo"),
        F.round(F.sum("sunhour"), 1).alias("sunhour_total_mes"),
        F.round(F.avg("sunhour"), 1).alias("sunhour_media_mes"),
        F.round(F.avg("duracao_dia"), 2).alias("duracao_dia_media"),
        F.round(F.avg("aproveitamento_solar"), 2).alias("aproveitamento_solar_media"),
        F.sum("dia_neve").alias("dias_neve"),
        F.round(F.max("heat_index"), 1).alias("heat_index_max_mes"),
        F.round(F.min("wind_chill"), 1).alias("wind_chill_min_mes"),
    )
    onda = maior_sequencia(daily, "dia_quente", "maior_onda_calor")
    seco = maior_sequencia(daily.withColumn("dia_seco", (F.col("precip_total") == 0).cast("int")),
                           "dia_seco", "maior_periodo_seco")
    monthly = (base
        .join(onda, ["district", "year_month"], "left")
        .join(seco, ["district", "year_month"], "left")
        .withColumn("maior_onda_calor", F.coalesce(F.col("maior_onda_calor"), F.lit(0)))
        .withColumn("maior_periodo_seco", F.coalesce(F.col("maior_periodo_seco"), F.lit(0)))
        .withColumn("amplitude_extrema_mes", F.round(F.col("temp_max_mes") - F.col("temp_min_mes"), 1)))
    w_mes = Window.partitionBy("year_month")
    return (monthly
        .withColumn("ranking_temp", F.rank().over(w_mes.orderBy(F.col("temp_media_mes").desc())))
        .withColumn("anomalia_temp", F.round(F.col("temp_media_mes") - F.avg("temp_media_mes").over(w_mes), 2)))

# ----------------------------------------------------------------------------
# Deteção do intervalo em falta
# ----------------------------------------------------------------------------
def meses_afetados(gap_start, target):
    """Lista de (ano, mes) que cobrem o intervalo [gap_start, target]."""
    meses, a, m = [], gap_start.year, gap_start.month
    while (a, m) <= (target.year, target.month):
        meses.append((a, m))
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)
    return meses

# ----------------------------------------------------------------------------
# Orquestração
# ----------------------------------------------------------------------------
def main():
    target = date.today() - timedelta(days=1)   # último dia totalmente disponível = ontem
    ultimo = spark.read.parquet(HDFS_SILVER).agg(F.max("date")).collect()[0][0]
    print(f"Último dia no silver: {ultimo} | alvo (ontem): {target}")

    gap_start = ultimo + timedelta(days=1)
    if gap_start > target:
        print("Sem dias novos a carregar. Nada a fazer.")
        return

    afetados = meses_afetados(gap_start, target)
    print(f"Dias em falta: {gap_start} a {target} | meses a reprocessar: {afetados}")

    # --- BRONZE: extrair os meses afetados (mês inteiro até ao alvo) e subir ao HDFS ---
    for ano, mes in afetados:
        end_day = target.day if (ano, mes) == (target.year, target.month) else calendar.monthrange(ano, mes)[1]
        print(f"[BRONZE] {ano}-{mes:02d} (dias 1..{end_day})")
        extrair_mes_bronze(ano, mes, end_day)
        put_mes_hdfs(ano, mes)

    # --- SILVER: reconstruir só os meses afetados (dynamic overwrite) ---
    partes = []
    for ano, mes in afetados:
        glob = f"{HDFS_BRONZE}/ano={ano:04d}/mes={mes:02d}/distrito=*/*.json"
        partes.append(silver_de_bronze(glob))
    silver_novo = partes[0]
    for p in partes[1:]:
        silver_novo = silver_novo.unionByName(p)
    silver_novo.coalesce(1).write.mode("overwrite").partitionBy("year_month").parquet(HDFS_SILVER)
    print("[SILVER] meses afetados atualizados.")

    # --- GOLD: ler o silver SÓ dos meses afetados e reconstruir daily + monthly ---
    yms = [f"{a:04d}-{m:02d}" for a, m in afetados]
    silver_aff = spark.read.parquet(HDFS_SILVER).filter(F.col("year_month").isin(yms))
    daily = build_daily(silver_aff)
    daily.coalesce(1).write.mode("overwrite").partitionBy("year_month").parquet(HDFS_GOLD_DAILY)
    monthly = build_monthly(daily)
    monthly.coalesce(1).write.mode("overwrite").partitionBy("year_month").parquet(HDFS_GOLD_MONTHLY)
    print("[GOLD] daily e monthly dos meses afetados atualizados.")
    print("Atualização concluída.")

if __name__ == "__main__":
    main()
