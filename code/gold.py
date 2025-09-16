###GOLD###

# gold.py — agregação da Silver -> Gold (Parquet final)
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def log(msg: str):
    print(f"[gold] {msg}", flush=True)

def main():
    spark = SparkSession.builder.appName("gold-processing").getOrCreate()
    conf = spark.conf
    BUCKET = conf.get("spark.driverEnv.BUCKET")
    RUN_ID = conf.get("spark.driverEnv.RUN_ID")
    if not BUCKET or not RUN_ID:
        raise RuntimeError("Env BUCKET/RUN_ID não definidos pelo Workflow.")

    emp_path = f"gs://{BUCKET}/silver/{RUN_ID}/empresas"
    soc_path = f"gs://{BUCKET}/silver/{RUN_ID}/socios"
    out_path = f"gs://{BUCKET}/gold/{RUN_ID}/resultado_final"

    log("lendo silver...")
    df_emp = spark.read.parquet(emp_path)
    df_soc = spark.read.parquet(soc_path)

    log("agregando socios...")
    df_qtde = df_soc.groupBy("cnpj").agg(F.count(F.lit(1)).alias("qtde_socios"))
    df_flag = (df_soc
               .withColumn("is_estrangeiro", F.when(F.col("tipo_socio") == 3, F.lit(1)).otherwise(F.lit(0)))
               .groupBy("cnpj").agg(F.max("is_estrangeiro").alias("flag_int"))
               .withColumn("flag_socio_estrangeiro", F.col("flag_int") == 1)
               .drop("flag_int"))

    log("join final e regras...")
    df_gold = (df_emp
               .join(df_qtde, "cnpj", "left")
               .join(df_flag, "cnpj", "left"))

    df_gold = df_gold.fillna({"qtde_socios": 0})
    df_gold = df_gold.fillna({"flag_socio_estrangeiro": False})

    df_gold = df_gold.withColumn(
        "doc_alvo",
        F.when((F.col("cod_porte") == F.lit("03")) & (F.col("qtde_socios") > 1), F.lit(True)).otherwise(F.lit(False))
    )

    df_final = df_gold.select(
        "cnpj", "razao_social", "natureza_juridica", "capital_social", "cod_porte",
        "qtde_socios", "flag_socio_estrangeiro", "doc_alvo"
    )

    log(f"gravando -> {out_path}")
    df_final.write.mode("overwrite").parquet(out_path)

    log("OK: gold concluído.")
    spark.stop()

if __name__ == "__main__":
    main()


