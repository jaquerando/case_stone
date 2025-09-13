from flask import Flask, request, jsonify
from google.cloud import storage
import requests, tempfile

app = Flask(__name__)
gcs = storage.Client()

def upload_url_to_gcs(url: str, bucket_name: str, gcs_path: str):
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    with tempfile.NamedTemporaryFile() as tmp:
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
        tmp.flush(); tmp.seek(0)
        blob.upload_from_filename(tmp.name)

@app.route("/", methods=["POST"])
def ingest():
    data = request.get_json(force=True, silent=False)

    for k in ["bucket", "prefix", "run_id", "urls", "marker"]:
        if k not in data or data[k] in (None, ""):
            return jsonify({"error": f"missing field: {k}"}), 400

    bucket = data["bucket"]
    prefix = data["prefix"].rstrip("/") + "/"
    run_id = data["run_id"]
    urls   = data["urls"]
    marker = data["marker"]

    saved = []
    for url in urls:
        base = url.split("?")[0].rstrip("/").split("/")[-1] or "file.bin"
        target = f"{prefix}{base}"
        upload_url_to_gcs(url, bucket, target)
        saved.append(target)

    gcs.bucket(bucket).blob(marker).upload_from_string(b"")
    return jsonify({"ok": True, "saved": saved, "marker": marker, "run_id": run_id})
