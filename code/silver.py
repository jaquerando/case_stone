###SILVER###

# silver.py — normalização da Bronze -> Silver (Parquet)
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

def log(msg: str):
    print(f"[silver] {msg}", flush=True)

def path_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("parquet").load(path).limit(1).collect()
        return True
    except Exception:
        return False

def rename_by_position(df, names):
    cols = df.columns
    # remove coluna auxiliar se existir
    if "_file" in cols:
        df = df.drop("_file")
        cols = df.columns
    need = len(names)
    if len(cols) < need:
        raise RuntimeError(f"DataFrame tem {len(cols)} colunas, mas eu preciso de {need}.")
    # pega as primeiras N na ordem
    selected = cols[:need]
    for i, new_name in enumerate(names):
        df = df.withColumnRenamed(selected[i], new_name)
    return df.select(names)

def read_empresas(spark, bucket, run_id):
    bronze_parq = f"gs://{bucket}/bronze/{run_id}/empresas/parquet"
    bronze_csv  = f"gs://{bucket}/bronze/{run_id}/empresas/csv/*.csv"

    if path_exists(spark, bronze_parq):
        log(f"lendo Parquet: {bronze_parq}")
        df = spark.read.parquet(bronze_parq)
    else:
        log(f"lendo CSV fallback: {bronze_csv}")
        schema = StructType([
            StructField("_c0", StringType(), True),
            StructField("_c1", StringType(), True),
            StructField("_c2", StringType(), True),
            StructField("_c3", StringType(), True),
            StructField("_c4", StringType(), True),
            StructField("_c5", StringType(), True),
            StructField("_c6", StringType(), True),
        ])
        df = (spark.read
              .option("sep", ";")
              .option("header", "false")
              .option("encoding", "latin1")
              .schema(schema)
              .csv(bronze_csv))

    names = [
        "cnpj", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social",
        "cod_porte", "ente_federativo_responsavel"
    ]
    df = rename_by_position(df, names)

    # casts
    df = df.withColumn(
        "capital_social",
        F.regexp_replace(F.col("capital_social"), ",", ".").cast(DoubleType())
    )
    for c in ["natureza_juridica", "qualificacao_responsavel"]:
        df = df.withColumn(c, F.col(c).cast(IntegerType()))
    return df

def read_socios(spark, bucket, run_id):
    bronze_parq = f"gs://{bucket}/bronze/{run_id}/socios/parquet"
    bronze_csv  = f"gs://{bucket}/bronze/{run_id}/socios/csv/*.csv"

    if path_exists(spark, bronze_parq):
        log(f"lendo Parquet: {bronze_parq}")
        df = spark.read.parquet(bronze_parq)
    else:
        log(f"lendo CSV fallback: {bronze_csv}")
        schema = StructType([
            StructField("_c0", StringType(), True),
            StructField("_c1", StringType(), True),
            StructField("_c2", StringType(), True),
            StructField("_c3", StringType(), True),
            StructField("_c4", StringType(), True),
            StructField("_c5", StringType(), True),
            StructField("_c6", StringType(), True),
            StructField("_c7", StringType(), True),
            StructField("_c8", StringType(), True),
            StructField("_c9", StringType(), True),
            StructField("_c10", StringType(), True),
        ])
        df = (spark.read
              .option("sep", ";")
              .option("header", "false")
              .option("encoding", "latin1")
              .schema(schema)
              .csv(bronze_csv))

    names = [
        "cnpj", "tipo_socio", "nome_socio", "documento_socio",
        "codigo_qualificacao_socio", "data_entrada_sociedade",
        "pais", "representante_legal", "nome_representante",
        "qualificacao_representante_legal", "faixa_etaria"
    ]
    df = rename_by_position(df, names)

    # casts
    df = df.withColumn("tipo_socio", F.col("tipo_socio").cast(IntegerType()))
    df = df.withColumn(
        "codigo_qualificacao_socio",
        F.col("codigo_qualificacao_socio").cast(IntegerType())
    )
    return df

def main():
    spark = SparkSession.builder.appName("silver-processing").getOrCreate()
    conf = spark.conf
    BUCKET = conf.get("spark.driverEnv.BUCKET")
    RUN_ID = conf.get("spark.driverEnv.RUN_ID")

    if not BUCKET or not RUN_ID:
        raise RuntimeError("Env BUCKET/RUN_ID não definidos pelo Workflow.")

    log(f"BUCKET={BUCKET} RUN_ID={RUN_ID}")

    df_emp = read_empresas(spark, BUCKET, RUN_ID)
    df_soc = read_socios(spark, BUCKET, RUN_ID)

    out_emp = f"gs://{BUCKET}/silver/{RUN_ID}/empresas"
    out_soc = f"gs://{BUCKET}/silver/{RUN_ID}/socios"

    log(f"gravando empresas -> {out_emp}")
    df_emp.write.mode("overwrite").parquet(out_emp)

    log(f"gravando socios -> {out_soc}")
    df_soc.write.mode("overwrite").parquet(out_soc)

    log("OK: silver concluído.")
    spark.stop()

if __name__ == "__main__":
    main()
