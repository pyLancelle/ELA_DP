# Archive des anciens fichiers Spotify

Ce dossier contient les anciennes versions de scripts qui ont été remplacées par des versions plus récentes.

## Fichiers archivés

### `spotify_ingest_old.py` (anciennement `spotify_ingest.py`)
- **Date d'archivage** : 2025-12-27
- **Raison** : Remplacé par `spotify_ingest.py`
- **Problème** : Utilisait un schéma universel et chargeait tout dans `lake_spotify__stg_spotify_raw`

### `spotify_ingest_auto_old.py` (anciennement `spotify_ingest_auto.py`)
- **Date d'archivage** : 2025-12-27
- **Raison** : Logique fusionnée dans l'adaptateur `src/connectors/ingestor/adapters/spotify.py`
- **Problème** : Couche intermédiaire inutile avec subprocess

## Architecture actuelle

```
Adaptateur (spotify.py) → SpotifyIngestor (spotify_ingest.py)
```

Un seul appel Python direct, sans subprocess.

## Notes

Ces fichiers sont conservés pour référence historique mais ne doivent plus être utilisés en production.
