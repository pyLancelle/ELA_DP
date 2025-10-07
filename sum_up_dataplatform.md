# Data Platform – Business Data Playbook
_Date: 2025-09-26_

Ce document synthétise la valeur métier dégagée par les modèles dbt du projet. Il répond aux questions : quelles informations possédons-nous ? comment les (ré)utiliser pour une application web personnelle ? quelles histoires les données racontent-elles ?

---

## 1. Garmin – Suivre le corps et la performance

### 1.1 Activités sportives
- **Vue clé** : `hub_garmin__activities`
- **Ce que l’on sait** : type de séance, calendrier exact, durée, distance, dénivelé, fréquences cardiaques, puissance, calories, météo et équipement. Un “profil” complet de chaque entraînement.
- **À raconter** : progression des volumes, comparaison des séances similaires, meilleures performances, charge d’entraînement vs fatigue.
- **Idées PWA** :
  - Ligne du temps des activités avec badges “records personnels”.
  - Carte interactive ou résumés par lieu (location struct).
  - Score de charge hebdo en cumulant `duration_seconds`, `intensity_minutes`, `body_battery_change`.

### 1.2 Running avancé
- **Vues** : `hub_garmin__activities_running_metrics`, `hub_garmin__activities_running_segments`, `hub_garmin__activities_running_timeseries`.
- **Ce que l’on sait** : cadence réelle, oscillation verticale, longueur de foulée, puissance, temps de contact au sol, segmentation des splits.
- **À raconter** : efficacité biomécanique, comparaison entre séances (ex : cadence vs vitesse), identification des segments les plus rapides, suivi de la régularité.
- **Idées PWA** :
  - Visualisation “radar” de la foulée par activité.
  - Alertes sur la dérive de cadence ou de puissance.
  - Classement perso des meilleurs segments.

### 1.3 Récupération & sommeil
- **Vues** : `hub_garmin__sleep`, `hub_garmin__sleep_timeseries`, `daily_recap__sleep`, `dashboard__sleep_*`.
- **Ce que l’on sait** : score de sommeil, durées par phase, HRV nocturne, stress, SpO₂, body battery, respiration minute par minute.
- **À raconter** : qualité de chaque nuit, déficits de sommeil, liens entre entraînements intenses et récupération, effets de la musique ou du stress.
- **Idées PWA** :
  - Carte “Sleep Scorecard” quotidienne avec insights textuels (`sleep_need.feedback`).
  - Tendances hebdo/mensuelles (moyenne des scores, variation HRV, pattern des réveils).
  - Corrélation “mauvaises nuits ↔ performances sportives ↔ musique écoutée”.

### 1.4 Bien-être continu
- **Vues** : `hub_garmin__body_battery`, `hub_garmin__body_battery_timeseries`, `hub_garmin__hrv`, `hub_garmin__stress`, `hub_garmin__steps_daily`, `hub_garmin__floors`, `hub_garmin__weight`.
- **Ce que l’on sait** : énergie disponible, HRV baseline, charge de stress quotidienne, activité quotidienne (pas, étages), variations de poids/composition.
- **À raconter** : équilibre charge/récupération, comportements sains, impact des programmes d’entraînement, moments critiques à surveiller (stress élevé, HRV bas).
- **Idées PWA** :
  - tableau de bord “Forme du jour” (body battery + HRV + stress + sommeil).
  - Alertes quand `training_status` passe en “UNPRODUCTIVE” ou quand `body_battery_change` est négatif plusieurs jours d’affilée.

### 1.5 Capacités & objectifs
- **Vues** : `hub_garmin__endurance_score`, `hub_garmin__hill_score`, `hub_garmin__race_predictions`, `hub_garmin__training_status`.
- **Ce que l’on sait** : indices de forme, projections chronométriques (5K, 10K, Semi, Marathon), recommandations d’entraînement.
- **À raconter** : progression des scores, fiabilité des prédictions (vs réalité), moment optimal pour tester une performance.
- **Idées PWA** :
  - Graphiques “score vs réalité” (ex : comparer `race_predictions` avec temps réels de `hub_garmin__activities`).
  - Assistant de planification (afficher `recovery_time_hours`, `training_recommendation`).

---

## 2. Spotify – Comprendre l’écoute réelle

### 2.1 Plays individuels
- **Vue clé** : `hub_spotify__recently_played`
- **Ce que l’on sait** : timestamp exact, morceau, artiste, album, contexte (playlist, appareil) et surtout **durée réellement écoutée** (`actual_duration_ms`), calculée en tenant compte du morceau suivant.
- **À raconter** : habitudes horaires, playlists les plus utilisées, titres qu’on “skippe” souvent, relation entre musique et autres activités (via jointure Garmin).
- **Idées PWA** :
  - Chronologie “player” avec barre de progression réelle.
  - Heatmap d’écoute (jour/heure) filtrable par humeur/playlist.
  - Score de complétion par titre (ratio écoute réelle/durée totale).

### 2.2 Agrégats mensuels
- **Vue** : `pct_spotify__listening_monthly_summary`
- **Ce que l’on sait** : minutes totales par mois, nombre de titres/artistes distincts, top track & top artist du mois.
- **À raconter** : évolution des volumes d’écoute, diversité musicale, périodes de découverte vs répétition.
- **Idées PWA** :
  - “Mix mensuel” avec carte d’identité (top artiste, top morceau, temps total).
  - Classement des mois les plus musicaux.

### 2.3 Classements temporels prêts à l’emploi
- **Vues** : `pct_spotify__listening_rankings_daily`, `_weekly`, `_yearly`
- **Ce que l’on sait** : pour chaque jour/semaine/année, top 20 tracks/artistes/albums avec minutes réellement écoutées et nombres de lectures.
- **À raconter** : comment les goûts évoluent, quelles sorties marquent une période, quels artistes dominent sur le long terme.
- **Idées PWA** :
  - “Hall of Fame” par période.
  - Graphiques comparant l’évolution du top 5 hebdo.

### 2.4 Emailing / Communication
- **Vues** : `pct_emailing__spotify_top10_tracks`, `_artists`, et versions “evolution”
- **Ce que l’on sait** : top 10 hebdo + mise en forme (temps d’écoute formaté, URL, vignettes). Les versions “evolution” indiquent les mouvements dans le classement (emoji, statut NEW/UP/DOWN).
- **À raconter** : newsletter perso, recap hebdo, notifications “ton artiste X repasse #1”.
- **À faire évoluer** : les modèles d’évolution utilisent encore la durée théorique du morceau (migrer vers `actual_duration_ms`).

### 2.5 Musique × Sport
- **Vue** : `pct_garmin__activities_spotify_tracks`
- **Ce que l’on sait** : pour chaque activité Garmin, la liste ordonnée des morceaux écoutés, le temps exact passé sur chaque track, le delta par rapport au début/fin de la séance.
- **À raconter** : bande-son des entraînements, playlists les plus efficaces, corrélation entre tempo et performance.
- **Idées PWA** :
  - Storytelling “Run mixtape” avec progression de la séance.
  - Analyse des BPM vs vitesse moyenne.

---

## 3. Chess – Brain Stats (à exploiter)

### 3.1 Parties
- **Vue** : `hub_chess__games`
- **Ce que l’on sait** : adversaires, cadence, résultat, durée, notation PGN.
- **À raconter** : forme aux échecs, temps de réflexion moyen, ouvertures préférées.
- **Idées PWA** :
  - Historique Elo vs fatigue (via HRV Garmin).
  - Highlights des meilleures parties.

### 3.2 Statistiques joueur
- **Vue** : `hub_chess__player_stats`
- **Ce que l’on sait** : scoreboard global (victoires/défaites/puzzles) pour chaque cadence.
- **À raconter** : zones de progression, objectifs (par exemple “passer 2200 en blitz”).

---

## 4. Cas d’usage PWA – Fiches produit rapides

| Besoin | Data sources | Proposition d’écran |
| --- | --- | --- |
| **Accueil « État du jour »** | `dashboard__sleep_latest`, `hub_garmin__hrv`, `hub_garmin__body_battery`, `hub_spotify__recently_played` | Carte “Sommeil”, indicateur HRV, énergie (body battery), dernier morceau joué, CTA “Voir mon programme du jour”. |
| **Journal d’entraînement** | `hub_garmin__activities`, `hub_garmin__activities_running_metrics`, `pct_garmin__activities_spotify_tracks` | Timeline avec stats clés + bande-son associée. Filtrer par sport, PR, type de séance. |
| **Explorateur musical** | `hub_spotify__recently_played`, `pct_spotify__listening_rankings_*`, `pct_spotify__listening_monthly_summary` | Mode “Daily/Weekly/Yearly recap”, comparateur d’artistes, graphes temps d’écoute. |
| **Récupération & bien-être** | `daily_recap__sleep`, `hub_garmin__stress`, `hub_garmin__body_battery_timeseries` | Dash multi-métriques (charts interactifs). Alertes si HRV/stress dérivent. |
| **Synthèse hebdomadaire automail** | `pct_emailing__spotify_top10_*`, `dashboard__sleep_1w`, `hub_garmin__training_status` | Génération d’un email HTML : top musique, score sommeil moyen, statut d’entraînement. |

---

## 5. Roadmap orientée valeur

1. **Aligner les classements “évolution” sur la durée réelle** pour une cohérence totale Spotify (intuitif pour la PWA/mails).
2. **Finaliser les timeseries running** pour exploiter toute la richesse biomécanique (sinon perte d’un différenciateur clé).
3. **Complexifier la narration sommeil** : agrégats hebdo/mensuels, corrélation avec corps & musique, recommandations personnalisées.
4. **Croiser les mondes** : créer des “insights” combinant Garmin, Spotify et Chess (ex : “les jours de parties gagnantes, tu écoutes davantage d’artistes calmes”).
5. **Construire une couche API** (FastAPI/Cloud Function) exposant des endpoints déjà filtrés pour la PWA : `/sleep/daily`, `/activities/latest`, `/spotify/top?granularity=weekly`, etc.
6. **Alerting intelligent** : seuils sur HRV, stress, charge, temps d’écoute (ex : “tu n’as pas couru depuis 5 jours”, “ta diversité musicale chute”).

---

## 6. Points de vigilance
- **Qualité** : ajouter des tests de cohérence (ex. `actual_duration_ms <= track.duration_ms + marge`).
- **Documentation vivante** : ce playbook doit évoluer avec les nouvelles données (ex. future intégration PWA, nouvelles sources).
- **Volumes** : surveiller les coûts BigQuery (daily/weekly rankings peuvent être lourds si l’historique grandit).
- **Sécurité** : gérer les secrets d’accès BigQuery en dehors du code (env vars) une fois la PWA déployée.

---

Ce résumé est pensé comme une base pour ton “assistant perso” multi-support. Chaque section pointe vers les données à exploiter, leur langage métier et les expériences concrètes à offrir.
