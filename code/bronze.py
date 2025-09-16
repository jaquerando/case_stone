# bronze.py — RFB CNPJ → Bronze (CSV + Parquet)
# Funciona em Dataproc Serverless (PySpark)
# Prioridade de origem:
# 1) URL_EMPRESAS / URL_SOCIOS (HTTP)
# 2) gs://BUCKET/raw/RUN_ID/*.zip
# 3) gs://BUCKET/bronze/RUN_ID/raw_zips/*.zip

import os
import re
import io
import zipfile
import tempfile
import urllib.request
from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# GCS client (Dataproc Serverless tem cred do SA)
from google.cloud import storage

def log(msg: str):
    print(f"[bronze] {msg}", flush=True)

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"env {name} ausente")
    return v

def detect_sep(sample_path: str) -> str:
    with open(sample_path, "rb") as f:
        head = f.read(4096)
    try:
        s = head.decode("latin-1", errors="ignore")
    except Exception:
        s = head.decode("utf-8", errors="ignore")
    counts = {c: s.count(c) for c in ["|", ";", ","]}
    sep = max(counts, key=counts.get)
    return sep if counts[sep] > 0 else ";"

def gcs_list(gs_bucket: str, prefix: str) -> List[str]:
    client = storage.Client()
    out = []
    for bl in client.list_blobs(gs_bucket, prefix=prefix):
        out.append(f"gs://{gs_bucket}/{bl.name}")
    return out

def gcs_download(gs_uri: str, dest_dir: str) -> str:
    m = re.match(r"^gs://([^/]+)/(.+)$", gs_uri)
    if not m:
        raise ValueError(f"URI GCS inválida: {gs_uri}")
    bucket, obj = m.group(1), m.group(2)
    client = storage.Client()
    blob = client.bucket(bucket).blob(obj)
    local_path = os.path.join(dest_dir, os.path.basename(obj))
    blob.download_to_filename(local_path)
    return local_path

def http_download(url: str, dest_dir: str) -> str:
    local_path = os.path.join(dest_dir, os.path.basename(url) or "file.zip")
    log(f"baixando: {url}")
    with urllib.request.urlopen(url) as r, open(local_path, "wb") as f:
        while True:
            chunk = r.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
    return local_path

def unzip_local_zip(zip_path: str, dest_dir: str) -> List[str]:
    csv_paths = []
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            if name.endswith("/") or not name.lower().endswith(".csv"):
                continue
            out = os.path.join(dest_dir, os.path.basename(name))
            with z.open(name) as src, open(out, "wb") as dst:
                dst.write(src.read())
            csv_paths.append(out)
    return csv_paths

def upload_csvs_to_gcs(csv_paths: List[str], bucket: str, gcs_prefix: str):
    client = storage.Client()
    b = client.bucket(bucket)
    for p in csv_paths:
        blob = b.blob(f"{gcs_prefix}/{os.path.basename(p)}")
        blob.upload_from_filename(p)
        log(f"CSV -> gs://{bucket}/{gcs_prefix}/{os.path.basename(p)}")

def read_all_to_df(spark: SparkSession, csv_paths: List[str]) -> Tuple[str, 'pyspark.sql.DataFrame']:
    if not csv_paths:
        raise RuntimeError("Nenhum CSV para ler.")
    sep = detect_sep(csv_paths[0])
    log(f"separador: '{sep}'")
    df = (
        spark.read
        .option("sep", sep)
        .option("header", "false")
        .option("encoding", "latin1")
        .option("inferSchema", "false")
        .csv(csv_paths)
        .withColumn("_file", input_file_name())
    )
    return sep, df

def choose_zip_groups(zip_uris: List[str]) -> Tuple[List[str], List[str]]:
    """Separa listas de zips de empresas e socios por heurística no nome."""
    emp, soc = [], []
    for u in zip_uris:
        low = u.lower()
        if "empresa" in low:
            emp.append(u)
        elif "socio" in low or "sócio" in low:
            soc.append(u)
    # fallback: se vier só 2 itens e nada casou, assume 1º=emp, 2º=soc
    if not emp and not soc and len(zip_uris) == 2:
        emp = [zip_uris[0]]
        soc = [zip_uris[1]]
    return emp, soc

def resolve_zip_sources(bucket: str, run_id: str, url_emp: str, url_soc: str) -> Tuple[List[str], List[str], str]:
    """
    Retorna (emp_zip_uris, soc_zip_uris, origem) onde zip_uris são:
      - caminhos locais (se URL) ou
      - gs://... (se GCS)
    """
    if url_emp or url_soc:
        origem = "HTTP"
        return ([url_emp] if url_emp else [], [url_soc] if url_soc else []), origem

    # tenta raw/<run_id> primeiro
    raw = gcs_list(bucket, f"raw/{run_id}/")
    zips_raw = [u for u in raw if u.lower().endswith(".zip")]
    if zips_raw:
        emp, soc = choose_zip_groups(zips_raw)
        if emp or soc:
            return (emp, soc), "GCS_RAW"

    # tenta bronze/<run_id>/raw_zips
    br = gcs_list(bucket, f"bronze/{run_id}/raw_zips/")
    zips_br = [u for u in br if u.lower().endswith(".zip")]
    if zips_br:
        emp, soc = choose_zip_groups(zips_br)
        if emp or soc:
            return (emp, soc), "GCS_BR_RAWZIPS"

    raise RuntimeError("Nenhuma fonte de ZIP encontrada (URLs vazias e GCS sem ZIPs em raw/ ou bronze/raw_zips/).")

def main():
    spark = SparkSession.builder.appName("bronze-rfb-cnpj").getOrCreate()

    RUN_ID = getenv_required("RUN_ID")
    BUCKET = getenv_required("BUCKET")
    URL_EMPRESAS = os.getenv("URL_EMPRESAS", "").strip()
    URL_SOCIOS   = os.getenv("URL_SOCIOS", "").strip()

    out_emp_csv = f"bronze/{RUN_ID}/empresas/csv"
    out_soc_csv = f"bronze/{RUN_ID}/socios/csv"
    out_emp_parq = f"gs://{BUCKET}/bronze/{RUN_ID}/empresas/parquet"
    out_soc_parq = f"gs://{BUCKET}/bronze/{RUN_ID}/socios/parquet"

    tmp = tempfile.mkdtemp(prefix="bronze_")
    log(f"RUN_ID={RUN_ID} BUCKET={BUCKET} TMP={tmp}")

    # Resolve fontes
    (emp_sources, soc_sources), origem = resolve_zip_sources(BUCKET, RUN_ID, URL_EMPRESAS, URL_SOCIOS)
    log(f"origem detectada: {origem}")
    log(f"empresas zips: {emp_sources}")
    log(f"socios zips:   {soc_sources}")

    # Baixa/obtém ZIPs para local
    local_emp_zips, local_soc_zips = [], []
    if origem == "HTTP":
        if URL_EMPRESAS:
            local_emp_zips.append(http_download(URL_EMPRESAS, tmp))
        if URL_SOCIOS:
            local_soc_zips.append(http_download(URL_SOCIOS, tmp))
    else:
        for u in emp_sources:
            local_emp_zips.append(gcs_download(u, tmp))
        for u in soc_sources:
            local_soc_zips.append(gcs_download(u, tmp))

    # Extrai CSVs locais
    emp_csvs, soc_csvs = [], []
    for z in local_emp_zips:
        emp_csvs += unzip_local_zip(z, tmp)
    for z in local_soc_zips:
        soc_csvs += unzip_local_zip(z, tmp)

    if not emp_csvs:
        raise RuntimeError("Empresas: nenhum CSV extraído.")
    if not soc_csvs:
        raise RuntimeError("Sócios: nenhum CSV extraído.")

    # Sobe CSVs crus para bronze/.../csv
    upload_csvs_to_gcs(emp_csvs, BUCKET, out_emp_csv)
    upload_csvs_to_gcs(soc_csvs, BUCKET, out_soc_csv)

    # Também escreve Parquet (útil pro Silver)
    _, df_emp = read_all_to_df(spark, emp_csvs)
    _, df_soc = read_all_to_df(spark, soc_csvs)

    log(f"Empresas: {df_emp.count()} linhas → {out_emp_parq}")
    df_emp.write.mode("overwrite").parquet(out_emp_parq)

    log(f"Sócios: {df_soc.count()} linhas → {out_soc_parq}")
    df_soc.write.mode("overwrite").parquet(out_soc_parq)

    log("OK: bronze concluído.")
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FALHA geral bronze: {e}")
        raise
