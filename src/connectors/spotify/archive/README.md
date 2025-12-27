# Archive des anciens fichiers Spotify

Ce dossier contient les anciennes versions de scripts qui ont été remplacées par des versions plus récentes.

## Fichiers archivés

### `spotify_ingest_old.py` (anciennement `spotify_ingest.py`)
- **Date d'archivage** : 2025-12-27
- **Raison** : Remplacé par `spotify_ingest_v2.py` + `spotify_ingest_auto.py`
- **Problème** : Utilisait un schéma universel et chargeait tout dans `lake_spotify__stg_spotify_raw`
- **Solution actuelle** : `spotify_ingest_v2.py` parse le JSON et charge dans les tables `lake_spotify__normalized_*` attendues par dbt

## Notes

Ces fichiers sont conservés pour référence historique mais ne doivent plus être utilisés en production.
