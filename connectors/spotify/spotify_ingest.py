import argparse
import json
from datetime import datetime, timezone
from google.cloud import bigquery, storage


def get_env_config(env: str):
    if env == "dev":
        return {
            "bucket": "ela-dp-dev",
            "bq_dataset": "ela_dp_dev",
        }
    elif env == "prd":
        return {
            "bucket": "ela-dp-prd",
            "bq_dataset": "ela_dp_prd",
        }
    else:
        raise ValueError("Env doit être 'dev' ou 'prd'.")


def list_gcs_files(bucket_name: str, prefix: str = "spotify/landing/") -> list:
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")
    ]


def move_gcs_file(bucket_name: str, source_path: str, dest_prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    filename = source_path.split("/")[-1]
    dest_path = f"spotify/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} déplacé vers {dest_path}")


def load_jsonl_with_metadata(uri: str, table_id: str, inserted_at: str):
    # Récupération des composants GCS
    parts = uri.split("/")
    bucket_name = parts[2]
    blob_path = "/".join(parts[3:])
    filename = parts[-1]

    # Lecture du fichier JSONL
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text().splitlines()

    rows = []
    for line in content:
        try:
            data = json.loads(line)
            data["dp_inserted_at"] = inserted_at
            data["source_file"] = filename
            rows.append(data)
        except json.JSONDecodeError:
            print(f"❌ Ligne invalide ignorée dans {filename}")
            continue

    if not rows:
        raise ValueError(f"Fichier vide ou invalide : {filename}")

    # Chargement dans BigQuery
    bq_client = bigquery.Client()
    job = bq_client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ),
    )
    job.result()
    print(f"✅ {filename} chargé avec {len(rows)} lignes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "prd"], required=True)
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    config = get_env_config(args.env)
    bucket = config["bucket"]
    dataset = config["bq_dataset"]
    table_id = f"{args.project}.{dataset}.staging_load"
    inserted_at = datetime.utcnow().isoformat()

    uris = list_gcs_files(bucket)
    print(f"🔍 Fichiers trouvés : {len(uris)}")

    for uri in uris:
        try:
            load_jsonl_with_metadata(uri, table_id, inserted_at)
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "archive")
        except Exception as e:
            print(f"❌ Erreur ingestion {uri} : {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")
