# dbt_dataplatform – Audit & Roadmap
_Date du rapport : 2025-02-14_

## 1. Synthèse exécutive
- **Architecture solide mais hétérogène** : le découpage lake → hub → product est respecté, mais le niveau de finition varie fortement selon les domaines (Garmin très détaillé vs. Spotify/Chess plus sommaires).
- **Données riches, gouvernance faible** : peu de tests, documentation lacunaire, absence d’exposures ou de catalogues utilisateurs, artefacts dbt versionnés.
- **Opportunités analytiques importantes** : fusion Garmin×Spotify déjà amorcée, top vues emailing, et potentiel encore inexploité (analyse de progression sportive, corrélations santé/musique, vision Chess).
- **Priorités recommandées** : sécuriser la qualité (tests, documentation), industrialiser les pipelines (state comparison, orchestrations), enrichir la couche produit (rapports multi-appareils, indicateurs de tendance), améliorer l’observabilité.

## 2. Cartographie actuelle

| Couche | Domaines | Observations clés |
| --- | --- | --- |
| **Lake** | garmin, spotify, chess | Ingestion JSON brute avec déduplication `ROW_NUMBER`. Config incrémentale cohérente mais dépend de l’horodatage `dp_inserted_at`. |
| **Hub** | garmin (~20 vues), spotify (4), chess (2) | Garmin fortement typé (STRUCTs, métriques complexes). Spotify hub récemment enrichi de la durée d’écoute réelle. Chess minimal. Peu de `schema.yml` hors Garmin. |
| **Product** | emailing, dashboard, daily_recap, garmin, spotify | Tableaux de bord sommeil/activité, emailing top 10, jointure Garmin×Spotify, nouveau récap mensuel. Règles métier dispersées, peu de mutualisation. |
| **Macros** | `get_schema` | Minimaliste ; pas de macros de nettoyage/dédup mutualisées. |
| **Tests** | `/tests/.gitkeep` | Pas de tests génériques ni unitaires. Les tests sont définis dans certains `schema.yaml` seulement. |
| **Artefacts** | `target/` versionné | Manifestes/compilés commités → risque de divergence & secrets. |

## 3. Qualité & gouvernance

### Tests
- Tests uniques/not_null présents sur quelques modèles (ex. `hub_garmin__activities`, `daily_recap__sleep`) mais absents ailleurs (Spotify hub/product, Chess).
- Aucun test de relations (ex. `product` reliant `hub`).
- Pas de tests de fraîcheur (`source freshness`) ni de packages (ex. `dbt_utils`).

### Documentation
- README générique.
- `schema.yaml` complet pour Garmin, succinct ou inexistant pour Spotify/Chess.
- Pas d’`exposures`, pas de `metrics` dbt, pas de data dictionary central.

### Observabilité/Opérations
- Profils `dev`/`prd` partagent la même configuration de dataset (`profiles.yml`) ; la macro `get_schema` segmente par préfixe mais massivement couplée au nom du target.
- Pas de gestion d’état automatisée (`dbt artifacts` non comparés).
- Pas de snapshots malgré des entités évolutives (training status, top artistes).
- `target/` et `logs/` versionnés → nettoyer & gitignore.

## 4. Analyse par domaine

### Garmin
- Couverture très large (activités, sommeil, HRV, body battery, etc.).
- Modèles timeseries complexes (`hub_garmin__activities_running_timeseries`) encore marqués « ne marche pas » : revoir la logique partant de `detailed_data` (certaines extractions directes, pas de correction d’échelle).
- Potentiel de vues produit additionnelles : performance versus programme, comparaison chronologique, agrégats hebdomadaires/mensuels (somme des intensités, courbes body battery).

### Spotify
- Hub récemment enrichi de durées réelles (`actual_duration_ms`) – excellent pour analyses de complétion.
- Vues emailing historiques utilisent encore `track.duration_ms` (évolution) → mettre à jour pour reflet réel.
- Possibilité de générer des vues de tendance (rolling 28 jours, ratio skip/completion, segmentation par contexte).
- Datasets top artistes/tracks (long terme, moyen terme) encore inexploités dans la couche produit.

### Chess
- Ingestion minimale (games, player_stats). Aucun produit en aval.
- Idées : statistiques Elo, temps moyen par coup, corrélations performance/activité physique.

## 5. Roadmap de développement

### 5.1. Quick wins (0–2 semaines)
1. **Hygiène dépôt** : ajouter `src/dbt_dataplatform/target/` et `logs/` au `.gitignore`, supprimer artefacts versionnés.
2. **Mise à jour vues emailing** : remplacer `SUM(track.duration_ms)` par `SUM(COALESCE(actual_duration_ms, track.duration_ms, 0))` dans
   - `pct_emailing__spotify_top10_tracks_evolution` (`src/dbt_dataplatform/models/product/emailing/pct_emailing__spotify_top10_tracks_evolution.sql`)
   - `pct_emailing__spotify_top10_artists_evolution` (`src/dbt_dataplatform/models/product/emailing/pct_emailing__spotify_top10_artists_evolution.sql`)
3. **Documentation de base** : personnaliser `README.md`, ajouter un chapitre sur les volumes/datasets cibles.
4. **Tests génériques essentiels** : pour chaque modèle product exposé, ajouter `not_null`/`unique` sur les identifiants, `relationships` vers les hubs (via `dbt_utils.relationships_where` si nécessaire).
5. **State diff** : créer un dossier `artifacts/prod` non versionné et intégrer dans la doc command `dbt ls --state`.

### 5.2. Court terme (2–4 semaines)
1. **Documentation systématique** : compléter `schema.yaml` pour Spotify (hub + product) et Chess ; inclure description + tests.
2. **Macro de déduplication** : factoriser la logique `ROW_NUMBER()` utilisée dans toutes les vues lake.
3. **Exposures** : déclarer les dashboards (sleep, run) et emailings pour tracer l’impact en aval.
4. **Source freshness** : définir `loaded_at_field` (`dp_inserted_at`) et des limites (`warn_after`, `error_after`) pour Garmin/Spotify.
5. **Snapshots** : capturer les entités à évolution lente (training status, race predictions) pour historique fiable.
6. **Qualification timeseries Garmin** : revoir `hub_garmin__activities_running_timeseries` en reprenant la note « ne marche pas » ; valider les `metricDescriptors` et facteurs de conversion.

### 5.3. Moyen terme (1–2 mois)
1. **Couches produit thématiques** :
   - Synthèse `sport × musique` élargie (corrélation intensité vs tempo, contextualisation par playlist).
   - Vue forme/charge chronique (ATL/CTL) à partir des temps Garmin.
   - Tableau de bord Chess vs forme physique (Elo vs fatigue, HRV).
2. **Metric layer dbt** : définir `metrics` (temps écouté, distance, sommeil) pour usage BI uniformisé.
3. **Tests personnalisés** : vérifier cohérence des durations (ex. `actual_duration_ms <= track.duration_ms + marge`), anomalies physiologiques (vo2 >= 0).
4. **Pilotage orchestrateur** : script ou workflow pour `dbt build` partiel (`state:modified+`), notifications Slack/e-mail.
5. **Data contracts / API** : si exposé via services mail/dashboards, définir schémas JSON, validations.

### 5.4. Long terme (>2 mois)
1. **Anaytique avancée** : scoring forme musicale, détection de patterns (ex. playlists spécifiques corrélées aux perfs).
2. **Machine learning** : features stores (ex. weekly HRV, run cadence) pour prédire fatigue, recommandations playlists adaptatives.
3. **Self-service** : catalogue de données (dbt docs en CI, portail interne), partage externalisé.
4. **Observabilité complète** : intégrer Elementary, dbt artifacts + ingestion dans un lakehouse (Ex: BigQuery tables de monitoring).

## 6. Recommandations transverses

| Sujet | Recommandation |
| --- | --- |
| **Gestion de configurations** | Externaliser les paramètres (plages temporelles emailing) pour éviter duplication SQL ; utiliser macros paramétrables. |
| **Nommage** | Harmoniser `play_id`/`activity_id`/`user_id`; documenter conventions (snake_case, `pct_` pour product, etc.). |
| **Performances** | Sur les jointures temps (Garmin×Spotify), ajouter indices LOGICAL via partition/cluster (ex. partition par `activity_date_local`). |
| **Sécurité** | S’assurer que `profiles.yml` n’est pas commité avec secrets ; valider l’utilisation de `GOOGLE_APPLICATION_CREDENTIALS`. |
| **Tests de charge** | Les modèles timeseries volumineux devraient être évalués (coût, partitionnement). |
| **Packaging** | Intégrer `dbt_utils`, `dbt_expectations` pour enrichir tests ; envisager `elementary-data` pour monitoring. |

## 7. Plan d’action proposé

1. **Semaine 1**
   - Nettoyer dépôt (`target/`), MAJ vues emailing, README.
   - Ajouter tests not_null/unique sur hubs/product principaux.
2. **Semaine 2**
   - Documenter Spotify/Chess, introduire macros de déduplication.
   - Ajuster hub Garmin timeseries.
3. **Semaine 3**
   - Configurer freshness + snapshots, créer exposures.
   - Script de comparaison d’état (prod vs dev).
4. **Semaine 4**
   - Livrer nouvelles vues produit prioritaires (ex. corrélations forme/musique).
   - Lancer POC monitoring (Elementary).

## 8. Annexe – Points d’amélioration ciblés

- `src/dbt_dataplatform/models/product/emailing/pct_emailing__spotify_top10_tracks_evolution.sql` et `pct_emailing__spotify_top10_artists_evolution.sql` : aligner sur `actual_duration_ms`.
- `src/dbt_dataplatform/models/hub/garmin/hub_garmin__activities_running_timeseries.sql` : finaliser extraction timeseries (notation TODO).
- `src/dbt_dataplatform/models/hub/spotify/schema.yaml` : compléter la documentation (actuellement vide).
- `src/dbt_dataplatform/README.md` : remplacer le template par une page projet détaillant conventions, environnements, commandes.
- `src/dbt_dataplatform/tests/` : ajouter véritables tests (`dbt test`) au-delà du `gitkeep`.
- `src/dbt_dataplatform/target/` : retirer du dépôt, ajouter au `.gitignore`, s’assurer que les artefacts sensibles ne sont pas exposés.

---

_N’oubliez pas d’exécuter `dbt docs generate` et de publier la documentation une fois les descriptions/tests enrichis pour maximiser l’adoption interne._
