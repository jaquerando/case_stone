# load_postgres.py — Carrega GOLD -> Cloud SQL Postgres com UPSERT
# Requisitos: jars do Cloud SQL Connector (Postgres) e do driver PostgreSQL no Dataproc Serverless.

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, BooleanType

def log(msg: str):
    print(f"[load] {msg}", flush=True)

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"env {name} ausente")
    return v

def cast_schema(df):
    # Garante tipos compatíveis com o Postgres
    cols = df.columns
    need = [
        "cnpj","razao_social","natureza_juridica","capital_social","cod_porte",
        "qtde_socios","flag_socio_estrangeiro","doc_alvo"
    ]
    missing = [c for c in need if c not in cols]
    if missing:
        raise RuntimeError(f"Colunas ausentes no GOLD: {missing}")

    df = (
        df
        .withColumn("cnpj", F.col("cnpj").cast(StringType()))
        .withColumn("razao_social", F.col("razao_social").cast(StringType()))
        .withColumn("natureza_juridica", F.col("natureza_juridica").cast(IntegerType()))
        .withColumn("capital_social", F.col("capital_social").cast(DoubleType()))
        .withColumn("cod_porte", F.col("cod_porte").cast(StringType()))
        .withColumn("qtde_socios", F.col("qtde_socios").cast(LongType()))
        .withColumn("flag_socio_estrangeiro", F.col("flag_socio_estrangeiro").cast(BooleanType()))
        .withColumn("doc_alvo", F.col("doc_alvo").cast(BooleanType()))
    )
    return df.select(need)

def jdbc_url(instance_conn_name: str, db_name: str, user: str, password: str) -> str:
    # Usa socketFactory do Cloud SQL Connector (Java)
    return (
        f"jdbc:postgresql:///{db_name}"
        f"?socketFactory=com.google.cloud.sql.postgres.SocketFactory"
        f"&cloudSqlInstance={instance_conn_name}"
        f"&user={user}&password={password}"
    )

def exec_sql_via_jvm(spark: SparkSession, url: str, sql: str):
    # Executa DDL/DML via JDBC usando os JARs carregados pelo Spark.
    jvm = spark._sc._jvm
    jprops = jvm.java.util.Properties()
    try:
        jvm.java.lang.Class.forName("org.postgresql.Driver")
    except Exception:
        pass
    conn = jvm.java.sql.DriverManager.getConnection(url, jprops)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()

def main():
    spark = SparkSession.builder.appName("load-postgres").getOrCreate()

    RUN_ID   = getenv_required("RUN_ID")
    BUCKET   = getenv_required("BUCKET")
    INSTANCE = getenv_required("INSTANCE_CONN_NAME")
    DB_NAME  = getenv_required("DB_NAME")
    DB_USER  = getenv_required("DB_USER")
    DB_PASS  = getenv_required("DB_PASS")
    DB_TABLE = os.getenv("DB_TABLE", "cnpj_resultado")

    gold_path   = f"gs://{BUCKET}/gold/{RUN_ID}/resultado_final"
    stage_table = f"{DB_TABLE}_stg_{RUN_ID.replace('-', '_')}"

    log(f"RUN_ID={RUN_ID} BUCKET={BUCKET}")
    log(f"Gold path: {gold_path}")
    log(f"Tabela destino: {DB_TABLE}")
    log(f"Tabela staging: {stage_table}")

    # Lê GOLD
    df = spark.read.parquet(gold_path)
    df = cast_schema(df)

    # Monta URL JDBC
    url = jdbc_url(INSTANCE, DB_NAME, DB_USER, DB_PASS)
    driver = "org.postgresql.Driver"

    # Cria tabela destino se não existir (PK em cnpj)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {DB_TABLE} (
        cnpj TEXT PRIMARY KEY,
        razao_social TEXT,
        natureza_juridica INTEGER,
        capital_social DOUBLE PRECISION,
        cod_porte TEXT,
        qtde_socios BIGINT,
        flag_socio_estrangeiro BOOLEAN,
        doc_alvo BOOLEAN,
        updated_at TIMESTAMPTZ DEFAULT now()
    );
    """
    exec_sql_via_jvm(spark, url, create_sql)
    log("Tabela destino garantida.")

    # Grava staging via JDBC (overwrite por RUN_ID)
    log("Escrevendo staging via JDBC...")
    (df.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", url)
        .option("dbtable", stage_table)
        .option("driver", driver)
        .save())
    log("Staging escrita.")

    # UPSERT (ON CONFLICT)
    upsert_sql = f"""
    INSERT INTO {DB_TABLE} (
        cnpj, razao_social, natureza_juridica, capital_social, cod_porte,
        qtde_socios, flag_socio_estrangeiro, doc_alvo, updated_at
    )
    SELECT
        cnpj, razao_social, natureza_juridica, capital_social, cod_porte,
        qtde_socios, flag_socio_estrangeiro, doc_alvo, now()
    FROM {stage_table}
    ON CONFLICT (cnpj) DO UPDATE SET
        razao_social = EXCLUDED.razao_social,
        natureza_juridica = EXCLUDED.natureza_juridica,
        capital_social = EXCLUDED.capital_social,
        cod_porte = EXCLUDED.cod_porte,
        qtde_socios = EXCLUDED.qtde_socios,
        flag_socio_estrangeiro = EXCLUDED.flag_socio_estrangeiro,
        doc_alvo = EXCLUDED.doc_alvo,
        updated_at = now();
    """
    exec_sql_via_jvm(spark, url, upsert_sql)
    log("UPSERT concluído.")

    # Drop staging
    drop_sql = f"DROP TABLE IF EXISTS {stage_table};"
    exec_sql_via_jvm(spark, url, drop_sql)
    log("Staging removida.")

    log("OK: load concluído.")
    spark.stop()

if __name__ == "__main__":
    main()
