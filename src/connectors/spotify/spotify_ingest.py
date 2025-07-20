import argparse
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import os

dev = False
if dev:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs_key.json"

spotify_schema = [
    bigquery.SchemaField(
        "context",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField(
                "external_urls",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "spotify", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField("href", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("type", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("uri", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    bigquery.SchemaField("played_at", "STRING", "NULLABLE", None, None, (), None),
    bigquery.SchemaField(
        "track",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField(
                "album",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "album_type", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "artists",
                        "RECORD",
                        "REPEATED",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "external_urls",
                                "RECORD",
                                "NULLABLE",
                                None,
                                None,
                                (
                                    bigquery.SchemaField(
                                        "spotify",
                                        "STRING",
                                        "NULLABLE",
                                        None,
                                        None,
                                        (),
                                        None,
                                    ),
                                ),
                                None,
                            ),
                            bigquery.SchemaField(
                                "href", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "id", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "name", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "type", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "uri", "STRING", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "available_markets", "STRING", "REPEATED", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "external_urls",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "spotify", "STRING", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "href", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "id", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "images",
                        "RECORD",
                        "REPEATED",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "height", "INTEGER", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "url", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "width", "INTEGER", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "name", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "release_date", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "release_date_precision",
                        "STRING",
                        "NULLABLE",
                        None,
                        None,
                        (),
                        None,
                    ),
                    bigquery.SchemaField(
                        "total_tracks", "INTEGER", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "type", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "uri", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField(
                "artists",
                "RECORD",
                "REPEATED",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "external_urls",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "spotify", "STRING", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "href", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "id", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "name", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "type", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "uri", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField(
                "available_markets", "STRING", "REPEATED", None, None, (), None
            ),
            bigquery.SchemaField(
                "disc_number", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "duration_ms", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "explicit", "BOOLEAN", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "external_ids",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "isrc", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField(
                "external_urls",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "spotify", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField("href", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("id", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField(
                "is_local", "BOOLEAN", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField("name", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField(
                "popularity", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "preview_url", "STRING", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "track_number", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField("type", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("uri", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", mode="NULLABLE"),
]


saved_tracks_schema = [
    bigquery.SchemaField("added_at", "STRING", "NULLABLE", None, None, (), None),
    bigquery.SchemaField(
        "track",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            bigquery.SchemaField(
                "album",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "album_type", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "artists",
                        "RECORD",
                        "REPEATED",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "external_urls",
                                "RECORD",
                                "NULLABLE",
                                None,
                                None,
                                (
                                    bigquery.SchemaField(
                                        "spotify",
                                        "STRING",
                                        "NULLABLE",
                                        None,
                                        None,
                                        (),
                                        None,
                                    ),
                                ),
                                None,
                            ),
                            bigquery.SchemaField(
                                "href", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "id", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "name", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "type", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "uri", "STRING", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "available_markets", "STRING", "REPEATED", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "external_urls",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "spotify", "STRING", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "href", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "id", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "images",
                        "RECORD",
                        "REPEATED",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "height", "INTEGER", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "url", "STRING", "NULLABLE", None, None, (), None
                            ),
                            bigquery.SchemaField(
                                "width", "INTEGER", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "name", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "release_date", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "release_date_precision",
                        "STRING",
                        "NULLABLE",
                        None,
                        None,
                        (),
                        None,
                    ),
                    bigquery.SchemaField(
                        "total_tracks", "INTEGER", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "type", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "uri", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField(
                "artists",
                "RECORD",
                "REPEATED",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "external_urls",
                        "RECORD",
                        "NULLABLE",
                        None,
                        None,
                        (
                            bigquery.SchemaField(
                                "spotify", "STRING", "NULLABLE", None, None, (), None
                            ),
                        ),
                        None,
                    ),
                    bigquery.SchemaField(
                        "href", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "id", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "name", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "type", "STRING", "NULLABLE", None, None, (), None
                    ),
                    bigquery.SchemaField(
                        "uri", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField(
                "available_markets", "STRING", "REPEATED", None, None, (), None
            ),
            bigquery.SchemaField(
                "disc_number", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "duration_ms", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "explicit", "BOOLEAN", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "external_ids",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "isrc", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField(
                "external_urls",
                "RECORD",
                "NULLABLE",
                None,
                None,
                (
                    bigquery.SchemaField(
                        "spotify", "STRING", "NULLABLE", None, None, (), None
                    ),
                ),
                None,
            ),
            bigquery.SchemaField("href", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("id", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField(
                "is_local", "BOOLEAN", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField("name", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField(
                "popularity", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "preview_url", "STRING", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField(
                "track_number", "INTEGER", "NULLABLE", None, None, (), None
            ),
            bigquery.SchemaField("type", "STRING", "NULLABLE", None, None, (), None),
            bigquery.SchemaField("uri", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    bigquery.SchemaField("dp_inserted_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("source_file", "STRING", mode="NULLABLE"),
]


def get_env_config(env: str):
    if env == "dev" or env == "prd":
        return {
            "bucket": f"ela-dp-{env}",
            "bq_dataset": f"dp_lake_{env}",
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


def detect_file_type(filename: str) -> str:
    """Detect if file contains saved_tracks or recently_played data."""
    if "saved_tracks" in filename:
        return "saved_tracks"
    else:
        return "recently_played"


def move_gcs_file(bucket_name: str, source_path: str, dest_prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_path)
    filename = source_path.split("/")[-1]
    dest_path = f"spotify/{dest_prefix}/{filename}"
    bucket.copy_blob(source_blob, bucket, dest_path)
    source_blob.delete()
    print(f"📁 {source_path} déplacé vers {dest_path}")


def load_jsonl_with_metadata(uri: str, table_id: str, inserted_at: str, file_type: str):
    from google.cloud import bigquery, storage
    import json

    parts = uri.split("/")
    bucket_name = parts[2]
    blob_path = "/".join(parts[3:])
    filename = parts[-1]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text().splitlines()

    rows = []
    for line in content:
        try:
            data = json.loads(line)

            # Vérifie la présence de track > album
            if "track" in data:
                if "album" not in data["track"] or data["track"]["album"] is None:
                    data["track"]["album"] = {}

                album = data["track"]["album"]

                # Cast release_date en string si nécessaire
                release_date = album.get("release_date")
                if release_date is not None and not isinstance(release_date, str):
                    data["track"]["album"]["release_date"] = str(release_date)

                # Force presence de 'artists' comme liste vide
                if "artists" not in album or album["artists"] is None:
                    data["track"]["album"]["artists"] = []

            # Ajout des métadonnées
            data["dp_inserted_at"] = inserted_at
            data["source_file"] = filename

            rows.append(data)

        except json.JSONDecodeError:
            print(f"❌ Ligne invalide ignorée dans {filename}")
            continue
        except Exception as e:
            print(f"⚠️ Erreur de cast dans {filename} : {e}")
            continue

    if not rows:
        raise ValueError(f"Fichier vide ou invalide : {filename}")

    # Choose schema based on file type
    schema = saved_tracks_schema if file_type == "saved_tracks" else spotify_schema

    bq_client = bigquery.Client()
    job = bq_client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
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
    inserted_at = datetime.utcnow().isoformat()

    uris = list_gcs_files(bucket)
    print(f"🔍 Fichiers trouvés : {len(uris)}")

    for uri in uris:
        try:
            filename = uri.split("/")[-1]
            file_type = detect_file_type(filename)

            # Choose table based on file type
            if file_type == "saved_tracks":
                table_id = f"{args.project}.{dataset}.staging_spotify_saved_tracks"
            else:
                table_id = f"{args.project}.{dataset}.staging_spotify"

            print(f"📊 Processing {file_type} file: {filename}")
            load_jsonl_with_metadata(uri, table_id, inserted_at, file_type)
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "archive")
        except Exception as e:
            print(f"❌ Erreur ingestion {uri} : {e}")
            source_path = "/".join(uri.split("/")[3:])
            move_gcs_file(bucket, source_path, "rejected")
