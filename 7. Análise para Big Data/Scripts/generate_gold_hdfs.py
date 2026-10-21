# =============================================================================
# generate_gold.py
# Lê o SILVER (1 linha por distrito x dia) e produz duas camadas GOLD:
#   - gold_daily   : KPIs ao nível do dia (valores diretos + derivados + flags)
#   - gold_monthly : KPIs agregados ao nível do mês (distrito x year_month)
# Ambas gravadas em parquet, particionadas por year_month (facilita o update
# incremental: recalcular um mês = sobrescrever só essa partição).
# =============================================================================

from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.getOrCreate()

# ----------------------------------------------------------------------------
# Caminhos e parâmetros configuráveis
# ----------------------------------------------------------------------------
SILVER       = "hdfs://hadoop-master:39000/2250055/silver"
GOLD_DAILY   = "hdfs://hadoop-master:39000/2250055/gold/gold_daily"
GOLD_MONTHLY = "hdfs://hadoop-master:39000/2250055/gold/gold_monthly"

LIMIAR_CHUVA_INTENSA = 10.0   # mm/dia a partir do qual conta como chuva intensa
BASE_GRAUS_DIA       = 18.0   # temperatura de conforto para os graus-dia (HDD/CDD)


# ----------------------------------------------------------------------------
# Funções auxiliares
# ----------------------------------------------------------------------------
def to_min(col):
    """Converte uma hora no formato '07:58 AM' em minutos desde a meia-noite."""
    ts = F.to_timestamp(col, "hh:mm a")
    return F.hour(ts) * 60 + F.minute(ts)


def heat_index(tc, rh):
    """Sensação de calor (°C) pela regressão de Rothfusz do NWS.
    Recebe temperatura em °C e humidade relativa em %. A fórmula é em °F,
    por isso converte-se de e para Fahrenheit. Só é significativa com calor
    e humidade; abaixo de 27°C devolve a própria temperatura."""
    tf = tc * 9.0 / 5.0 + 32.0
    hi_f = (-42.379 + 2.04901523 * tf + 10.14333127 * rh
            - 0.22475541 * tf * rh - 0.00683783 * tf * tf
            - 0.05481717 * rh * rh + 0.00122874 * tf * tf * rh
            + 0.00085282 * tf * rh * rh - 0.00000199 * tf * tf * rh * rh)
    hi_c = (hi_f - 32.0) * 5.0 / 9.0
    return F.when(tc < 27, tc).otherwise(F.round(hi_c, 1))


def wind_chill(tc, v_kmh):
    """Sensação de frio (°C) pela fórmula NWS/Environment Canada (2001).
    Recebe temperatura em °C e vento em km/h. Só é significativa com frio
    e vento (T <= 10°C e V > 4.8 km/h); fora disso devolve a temperatura."""
    vp = F.pow(v_kmh, F.lit(0.16))
    wc = 13.12 + 0.6215 * tc - 11.37 * vp + 0.3965 * tc * vp
    return F.when((tc <= 10) & (v_kmh > 4.8), F.round(wc, 1)).otherwise(tc)


def maior_sequencia(df, flag_col, alias):
    """Maior nº de dias CONSECUTIVOS em que flag_col == 1, por distrito e mês.
    Usa o padrão 'gaps and islands': a diferença entre a ordem global dos dias
    e a ordem dos dias que cumprem a condição é constante dentro de cada
    sequência ininterrupta, servindo de chave de grupo."""
    w_all  = Window.partitionBy("district", "year_month").orderBy("date")
    w_flag = Window.partitionBy("district", "year_month", flag_col).orderBy("date")
    return (df
            .withColumn("_rn1", F.row_number().over(w_all))
            .withColumn("_rn2", F.row_number().over(w_flag))
            .withColumn("_grp", F.col("_rn1") - F.col("_rn2"))
            .filter(F.col(flag_col) == 1)
            .groupBy("district", "year_month", "_grp").agg(F.count("*").alias("_len"))
            .groupBy("district", "year_month").agg(F.max("_len").alias(alias)))


# ----------------------------------------------------------------------------
# 1) Ler o SILVER
# ----------------------------------------------------------------------------
silver = spark.read.parquet(SILVER)


# ----------------------------------------------------------------------------
# 2) GOLD DAILY — 1 linha por distrito x dia
# ----------------------------------------------------------------------------
daily = silver.select(
    # --- identificação ---
    "district", "country", "lat", "lon", "date",
    F.date_format("date", "yyyy-MM").alias("year_month"),

    # --- valores diretos (renomeados para PT) ---
    F.col("maxtemp").alias("temp_max"),
    F.col("mintemp").alias("temp_min"),
    F.col("avgtemp").alias("temp_media"),
    F.col("humidity_media").alias("humidade_media"),
    F.col("humidity_max").alias("humidade_max"),
    F.col("humidity_min").alias("humidade_min"),
    F.col("wind_media").alias("vento_media"),
    F.col("wind_max").alias("vento_max"),
    F.col("rajada_max"),
    F.col("uv_index"),
    F.col("sunhour"),
    F.col("precip_total"),

    # --- derivados ---
    F.round(F.col("maxtemp") - F.col("mintemp"), 1).alias("amplitude_termica"),
    F.round(F.col("humidity_max") - F.col("humidity_min"), 1).alias("amplitude_humidade"),
    F.round((to_min("sunset") - to_min("sunrise")) / 60.0, 2).alias("duracao_dia"),
    heat_index(F.col("maxtemp"), F.col("humidity_media")).alias("heat_index"),
    wind_chill(F.col("mintemp"), F.col("wind_media")).alias("wind_chill"),

    # categoria UV (escala da OMS)
    F.when(F.col("uv_index") <= 2, "baixo")
     .when(F.col("uv_index") <= 5, "moderado")
     .when(F.col("uv_index") <= 7, "alto")
     .when(F.col("uv_index") <= 10, "muito_alto")
     .otherwise("extremo").alias("categoria_uv"),

    # categoria vento (escala simples, km/h)
    F.when(F.col("wind_media") <= 20, "fraco")
     .when(F.col("wind_media") <= 50, "moderado")
     .when(F.col("wind_media") <= 70, "forte")
     .otherwise("muito_forte").alias("categoria_vento"),

    # --- flags 0/1 (base para as contagens mensais) ---
    (F.col("maxtemp") > 30).cast("int").alias("dia_quente"),
    (F.col("mintemp") < 5).cast("int").alias("dia_frio"),
    (F.col("totalsnow") > 0).cast("int").alias("dia_neve"),
    (F.col("precip_total") > 0).cast("int").alias("dia_chuva"),
    (F.col("precip_total") > LIMIAR_CHUVA_INTENSA).cast("int").alias("dia_chuva_intensa"),
)

# aproveitamento_solar depende de duracao_dia (calculada acima), por isso é
# adicionado num passo seguinte; protege contra divisão por zero.
daily = daily.withColumn(
    "aproveitamento_solar",
    F.when(F.col("duracao_dia") > 0,
           F.round(F.col("sunhour") / F.col("duracao_dia"), 2)),
)

daily.coalesce(1).write.mode("overwrite").partitionBy("year_month").parquet(GOLD_DAILY)
print(f"gold_daily escrito em {GOLD_DAILY}")


# ----------------------------------------------------------------------------
# 3) GOLD MONTHLY — 1 linha por distrito x mês
# ----------------------------------------------------------------------------
# 3a) Agregações diretas a partir do daily
base = daily.groupBy("district", "year_month").agg(
    F.first("country").alias("country"),
    F.first("lat").alias("lat"),
    F.first("lon").alias("lon"),

    # temperatura
    F.round(F.max("temp_max"), 1).alias("temp_max_mes"),
    F.round(F.min("temp_min"), 1).alias("temp_min_mes"),
    F.round(F.avg("temp_media"), 1).alias("temp_media_mes"),
    F.sum("dia_quente").alias("dias_quentes"),
    F.sum("dia_frio").alias("dias_frios"),
    F.round(F.avg("amplitude_termica"), 1).alias("amplitude_media_dia"),
    F.round(F.stddev("temp_media"), 2).alias("temp_desvio"),
    # graus-dia: soma diária do desvio à temperatura de conforto
    F.round(F.sum(F.greatest(F.lit(0.0), F.lit(BASE_GRAUS_DIA) - F.col("temp_media"))), 1)
        .alias("graus_dia_aquecimento"),
    F.round(F.sum(F.greatest(F.lit(0.0), F.col("temp_media") - F.lit(BASE_GRAUS_DIA))), 1)
        .alias("graus_dia_arrefecimento"),

    # precipitação
    F.round(F.sum("precip_total"), 1).alias("precip_total_mes"),
    F.sum("dia_chuva").alias("dias_chuva"),
    F.sum("dia_chuva_intensa").alias("dias_chuva_intensa"),
    F.round(F.max("precip_total"), 1).alias("precip_max_dia_mes"),

    # humidade
    F.round(F.avg("humidade_media"), 1).alias("humidade_media_mes"),
    F.round(F.max("humidade_max"), 1).alias("humidade_max_mes"),
    F.round(F.min("humidade_min"), 1).alias("humidade_min_mes"),
    F.sum(F.when(F.col("humidade_media") > 80, 1).otherwise(0)).alias("dias_humidos"),
    F.sum(F.when(F.col("humidade_media") < 30, 1).otherwise(0)).alias("dias_secos"),
    F.round(F.avg("amplitude_humidade"), 1).alias("amplitude_humidade_media"),

    # vento
    F.round(F.avg("vento_media"), 1).alias("vento_media_mes"),
    F.round(F.max("vento_max"), 1).alias("vento_max_mes"),
    F.round(F.max("rajada_max"), 1).alias("rajada_max_mes"),
    F.sum(F.when(F.col("vento_media") > 50, 1).otherwise(0)).alias("dias_vento_forte"),
    # distribuição pela escala simples
    F.sum(F.when(F.col("categoria_vento") == "fraco", 1).otherwise(0)).alias("vento_cat_fraco"),
    F.sum(F.when(F.col("categoria_vento") == "moderado", 1).otherwise(0)).alias("vento_cat_moderado"),
    F.sum(F.when(F.col("categoria_vento") == "forte", 1).otherwise(0)).alias("vento_cat_forte"),
    F.sum(F.when(F.col("categoria_vento") == "muito_forte", 1).otherwise(0)).alias("vento_cat_muito_forte"),

    # UV
    F.round(F.max("uv_index"), 1).alias("uv_max_mes"),
    F.round(F.avg("uv_index"), 1).alias("uv_media_mes"),
    F.sum(F.when(F.col("uv_index") >= 6, 1).otherwise(0)).alias("dias_uv_alto"),
    F.sum(F.when(F.col("uv_index") >= 8, 1).otherwise(0)).alias("dias_uv_extremo"),

    # horas de sol / luz
    F.round(F.sum("sunhour"), 1).alias("sunhour_total_mes"),
    F.round(F.avg("sunhour"), 1).alias("sunhour_media_mes"),
    F.round(F.avg("duracao_dia"), 2).alias("duracao_dia_media"),
    F.round(F.avg("aproveitamento_solar"), 2).alias("aproveitamento_solar_media"),

    # neve
    F.sum("dia_neve").alias("dias_neve"),

    # sensações
    F.round(F.max("heat_index"), 1).alias("heat_index_max_mes"),
    F.round(F.min("wind_chill"), 1).alias("wind_chill_min_mes"),
)

# 3b) Sequências consecutivas (window) — onda de calor e período seco
onda = maior_sequencia(daily, "dia_quente", "maior_onda_calor")
daily_seco = daily.withColumn("dia_seco", (F.col("precip_total") == 0).cast("int"))
seco = maior_sequencia(daily_seco, "dia_seco", "maior_periodo_seco")

monthly = (base
    .join(onda, ["district", "year_month"], "left")
    .join(seco, ["district", "year_month"], "left")
    # meses sem dias quentes / sem dias secos não têm sequência -> 0
    .withColumn("maior_onda_calor", F.coalesce(F.col("maior_onda_calor"), F.lit(0)))
    .withColumn("maior_periodo_seco", F.coalesce(F.col("maior_periodo_seco"), F.lit(0)))
    # amplitude extrema do mês (máx das máximas - mín das mínimas)
    .withColumn("amplitude_extrema_mes",
                F.round(F.col("temp_max_mes") - F.col("temp_min_mes"), 1)))

# 3c) Comparativos entre distritos no mesmo mês (window por year_month)
w_mes = Window.partitionBy("year_month")
monthly = (monthly
    # ranking: 1 = distrito mais quente desse mês
    .withColumn("ranking_temp",
                F.rank().over(w_mes.orderBy(F.col("temp_media_mes").desc())))
    # anomalia: desvio face à média nacional desse mês
    .withColumn("anomalia_temp",
                F.round(F.col("temp_media_mes") - F.avg("temp_media_mes").over(w_mes), 2)))

monthly.coalesce(1).write.mode("overwrite").partitionBy("year_month").parquet(GOLD_MONTHLY)
print(f"gold_monthly escrito em {GOLD_MONTHLY}")
print(f"daily: {daily.count()} linhas | monthly: {monthly.count()} linhas")
