import argparse
from google.cloud import bigquery, storage

PROJECT_ID = "ton-projet-gcp"  # Remplace avec ton ID projet réel


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


def list_gcs_files(bucket_name: str, prefix: str = "spotify/raw/") -> list:
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")
    ]


def load_into_bigquery(table_id: str, file_uris: list[str]):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND",  # ou "WRITE_TRUNCATE" si tu veux écraser
    )
    load_job = client.load_table_from_uri(file_uris, table_id, job_config=job_config)
    load_job.result()  # attend la fin du job
    print(f"✅ {len(file_uris)} fichiers chargés dans {table_id}")


def move_gcs_file(bucket_name: str, source_path: str, dest_path: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    bucket.copy_blob(source_blob, bucket, new_name=dest_path)
    source_blob.delete()
    print(f"📁 {source_path} déplacé vers {dest_path}")


def move_all_files(bucket_name: str, uris: list[str], target_prefix: str):
    for uri in uris:
        path = uri.replace(f"gs://{bucket_name}/", "")
        filename = path.split("/")[-1]
        dest_path = f"spotify/{target_prefix}/{filename}"
        move_gcs_file(bucket_name, path, dest_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "prd"], required=True)
    parser.add_argument("--project", required=True, help="GCP project ID")
    args = parser.parse_args()

    config = get_env_config(args.env)
    bucket = f"ela-dp-{args.env}/landing"
    bq_dataset = f"lake_spotify_{args.env}"
    bq_table = f"{args.project}.{bq_dataset}.normalized"

    print(f"🔍 Listing des fichiers dans gs://{bucket}/spotify/landing/")
    uris = list_gcs_files(bucket)

    try:
        if not uris:
            print("📭 Aucun fichier trouvé.")
        else:
            print(f"📤 Ingestion de {len(uris)} fichiers dans {bq_table}")
            load_into_bigquery(bq_table, uris)
            move_all_files(bucket, uris, "archive")
    except Exception as e:
        print(f"❌ Échec ingestion : {e}")
        move_all_files(bucket, uris, "rejected")
        raise
