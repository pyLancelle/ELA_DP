# Plan d'Implémentation : AI Coach pour Running Training

## Vision & Objectifs

Créer un **AI Coach** qui génère des plans d'entraînement hebdomadaires personnalisés en exploitant :
- Les données de santé Garmin (sommeil, HRV, stress, body battery, FC repos)
- Les activités sportives (GPS, zones cardio, charge d'entraînement)
- Un document de contexte utilisateur (objectifs, contraintes, préférences)
- Une approche hybride : plan de cycle (4-12 semaines) + régénération hebdomadaire adaptative

**LLM** : Claude Opus 4.5 (API Anthropic)
**Interface** : API REST pure intégrée dans FastAPI existante
**Stockage contexte** : Google Cloud Storage + API upload

## Use Cases MVP (Phase Initiale)

### Setup Initial (Manuel, 1x par cycle)
1. **Génération du profil coureur** : Analyser 90 jours d'historique pour créer un profil détaillé (niveau, forces/faiblesses, zones, tendances)
2. **Génération de cycle complet** : Créer un plan d'entraînement de 4-12 semaines avec vue hebdomadaire
3. **Configuration cycle** : Éditer `cycle_config.yaml` pour activer le nouveau cycle

### Automatisation Hebdomadaire (CRON Dimanche 21h)
4. **Weekly Review** : Analyse complète de la semaine écoulée (prévu vs réalisé, santé, sommeil) → `.md` généré
5. **Weekly Plan Adapté** : Génération du plan semaine suivante basé sur review + cycle + philosophie d'entraînement → `.md` généré

**Approche** :
- **Single-user hardcodé** : `user_id = "user_etienne"` partout, aucune auth pour MVP
- **Orchestration automatique** : Cloud Scheduler CRON trigger l'orchestrateur chaque dimanche
- **Cycle config YAML** : Fichier source of truth pour gérer les cycles actifs et leur organisation
- **Stockage hybride** : BigQuery (analytics) + GCS (fichiers .md lisibles)
- **Philosophie d'entraînement** : Intégrée dans le contexte (ex: 80% Zone 2, max 2 hard sessions/semaine)
- **Tests complets** : Couverture unitaire + intégration + mocks Claude

## Architecture Globale

### Intégration dans l'Architecture Existante

```
Flux Actuel :
Garmin API → Fetcher → GCS Landing → Ingestor → BigQuery (normalized/lake/hub/product) → FastAPI

Nouveau Flux AI Coach (Automatisé Hebdo) :
┌─────────────────────────────────────────────────────────┐
│  Cloud Scheduler (CRON - Dimanche 21h)                  │
│  └─> POST /api/ai-coach/orchestrate-weekly              │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  ORCHESTRATEUR (/src/services/ai_coach/orchestrator.py) │
│                                                          │
│  1. Lire cycle_config.yaml (GCS) → cycle actif ?        │
│  2. Récupère plan semaine écoulée (BigQuery)            │
│  3. Récupère activités réelles (BigQuery)               │
│  4. Récupère santé/sommeil 7j (BigQuery)                │
│  5. Claude → Weekly Review (.md complet)                │
│  6. Claude → Weekly Plan adapté                         │
│  7. Store review + plan (BigQuery + GCS .md)            │
└─────────────────────────────────────────────────────────┘
```

### Nouveaux Composants

**1. Service Layer** : `/src/services/ai_coach/` (10 modules)
- `config.py` : Configuration (constante `USER_ID = "user_etienne"`)
- `anthropic_client.py` : Wrapper Claude API avec tracking tokens
- `data_aggregator.py` : Agrégation intelligente données BigQuery
- `prompt_builder.py` : Construction prompts structurés
- `profile_generator.py` : Génération profil coureur
- `plan_generator.py` : Génération cycle complet + plans hebdo
- `weekly_reviewer.py` ⭐ : Génération weekly review (.md)
- `orchestrator.py` ⭐ : Orchestration hebdo complète (review + plan)
- `response_parser.py` : Parsing et validation réponses Claude
- `gcs_manager.py` : Upload/retrieval GCS (contextes, config YAML, .md)

**2. API Layer** : `/api/routers/ai_coach.py`
- 7 endpoints REST simplifiés (vs 12 initialement)
- Pydantic models : `/api/models/ai_coach.py`

**3. Data Layer** : BigQuery `dp_product_dev/prd`
- 6 nouvelles tables (profils, cycles, plans, reviews, contexts, executions)

**4. Storage** : Google Cloud Storage
- Bucket : `ela-dataplatform-ai-coach-contexts`
- **Cycle Config YAML** ⭐ : Source of truth pour cycles actifs
- **Organisation par cycle** : Chaque cycle a son dossier (reviews/, plans/)
- **Stockage hybride** : .md dans GCS + contenu dans BigQuery

## Cycle Config YAML (Source of Truth)

### Fichier : `gs://ela-dataplatform-ai-coach-contexts/user_etienne/cycle_config.yaml`

```yaml
# Configuration du cycle actif - édité manuellement lors du setup d'un nouveau cycle
active_cycle:
  cycle_id: "ecotrail-paris-2026"
  name: "Préparation Ecotrail Paris 80km"
  start_date: "2026-01-06"
  end_date: "2026-03-23"
  race_date: "2026-03-21"

  # Références aux ressources (chemins relatifs dans le bucket)
  context_file: "contexts/ecotrail_2026.json"
  profile_file: "profiles/profile_2026-01-05.json"
  cycle_plan_file: "cycles/ecotrail-2026/cycle_plan.json"

  # Organisation des outputs (généré automatiquement chaque semaine)
  outputs:
    reviews_folder: "cycles/ecotrail-2026/reviews"
    plans_folder: "cycles/ecotrail-2026/plans"

# Historique des cycles passés (pour référence)
past_cycles:
  - cycle_id: "marathon-automne-2025"
    name: "Marathon Paris Sub 3h45"
    start_date: "2025-09-01"
    end_date: "2025-11-15"
    race_date: "2025-11-10"
    status: "completed"
    final_result: "3:42:18"
```

**Usage** :
- L'orchestrateur lit ce fichier à chaque exécution hebdo
- Si `today` n'est pas dans `[start_date, end_date]` → sortie silencieuse (pas de cycle actif)
- Sinon → charge les ressources du cycle et génère review + plan dans les bons folders

## Structure Google Cloud Storage

```
gs://ela-dataplatform-ai-coach-contexts/
└── user_etienne/
    ├── cycle_config.yaml ⭐ (source of truth - édité manuellement)
    │
    ├── contexts/
    │   ├── ecotrail_2026.json
    │   └── marathon_paris_2026.json
    │
    ├── profiles/
    │   ├── profile_2026-01-05.json
    │   └── profile_2026-07-30.json
    │
    └── cycles/
        ├── ecotrail-2026/
        │   ├── cycle_plan.json (généré par generate-cycle)
        │   ├── reviews/
        │   │   ├── week_01_review.md ⭐ (généré chaque dimanche)
        │   │   ├── week_02_review.md
        │   │   └── ...
        │   └── plans/
        │       ├── week_02_plan.md ⭐ (plan pour semaine suivante)
        │       ├── week_03_plan.md
        │       └── ...
        │
        └── marathon-2026/
            ├── cycle_plan.json
            ├── reviews/
            └── plans/
```

## Schémas BigQuery (6 Tables)

### Table 1 : `ai_coach__runner_profiles`
Stocke les profils coureurs générés par l'IA.

```sql
CREATE TABLE dp_product_dev.ai_coach__runner_profiles (
  profile_id STRING NOT NULL,
  user_id STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,

  -- Métadonnées génération
  generated_by STRING,                    -- "claude-opus-4.5-20251101"
  generation_prompt_tokens INT64,
  generation_completion_tokens INT64,

  -- Période d'analyse
  analysis_start_date DATE,
  analysis_end_date DATE,
  total_activities_analyzed INT64,

  -- Profil complet (JSON)
  profile_json STRING,

  -- Insights clés (extraction pour requêtes rapides)
  runner_level STRING,                    -- "beginner"|"intermediate"|"advanced"|"elite"
  weekly_volume_km FLOAT64,
  vo2_max_estimate FLOAT64,
  primary_strengths ARRAY<STRING>,
  primary_weaknesses ARRAY<STRING>,
  recommended_training_zones STRUCT<
    zone1_hr_range STRING,
    zone2_hr_range STRING,
    zone3_hr_range STRING,
    zone4_hr_range STRING,
    zone5_hr_range STRING
  >,

  is_active BOOLEAN,                      -- Un seul profil actif par user
  _dp_updated_at TIMESTAMP
)
CLUSTER BY user_id, is_active;
```

### Table 2 : `ai_coach__training_cycles`
Stocke les plans de cycle complets (4-12 semaines).

```sql
CREATE TABLE dp_product_dev.ai_coach__training_cycles (
  cycle_id STRING NOT NULL,
  user_id STRING NOT NULL,
  profile_id STRING NOT NULL,
  context_gcs_path STRING NOT NULL,       -- gs://bucket/user/contexts/uuid.json
  created_at TIMESTAMP NOT NULL,

  -- Définition du cycle
  cycle_start_date DATE NOT NULL,
  cycle_end_date DATE NOT NULL,
  cycle_goal STRING,                      -- "marathon_sub_3h30", "build_base"
  total_weeks INT64,

  -- Plan complet (JSON)
  cycle_plan_json STRING,

  -- Résumés hebdomadaires
  weekly_summaries ARRAY<STRUCT<
    week_number INT64,
    week_start_date DATE,
    total_km FLOAT64,
    key_workouts ARRAY<STRING>,
    focus STRING
  >>,

  -- Métadonnées
  generated_by STRING,
  generation_prompt_tokens INT64,
  generation_completion_tokens INT64,

  status STRING,                          -- "active"|"completed"|"abandoned"
  _dp_updated_at TIMESTAMP
)
CLUSTER BY user_id, status;
```

### Table 3 : `ai_coach__weekly_plans`
Stocke les plans hebdomadaires (7 jours) régénérés chaque semaine.

```sql
CREATE TABLE dp_product_dev.ai_coach__weekly_plans (
  plan_id STRING NOT NULL,
  cycle_id STRING,                        -- FK (nullable pour plans standalone)
  user_id STRING NOT NULL,
  profile_id STRING NOT NULL,
  context_gcs_path STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,

  -- Semaine
  week_start_date DATE NOT NULL,
  week_end_date DATE NOT NULL,
  week_number_in_cycle INT64,

  -- Métadonnées
  generated_by STRING,
  generation_prompt_tokens INT64,
  generation_completion_tokens INT64,
  regeneration_reason STRING,             -- "initial"|"weekly_update"|"user_feedback"

  -- Snapshots d'input (pour traçabilité)
  input_health_snapshot STRING,           -- JSON des 7 derniers jours de santé
  input_activity_snapshot STRING,         -- JSON des 7 dernières activités

  -- Plan hebdomadaire (JSON complet)
  weekly_plan_json STRING,

  -- Plan Markdown ⭐ NOUVEAU
  plan_markdown STRING,                   -- Le .md du plan généré par Claude
  gcs_markdown_path STRING,               -- gs://bucket/.../plans/week_XX_plan.md

  -- Séances structurées (pour accès API facile)
  daily_workouts ARRAY<STRUCT<
    date DATE,
    day_name STRING,
    workout_type STRING,                  -- "easy_run"|"intervals"|"long_run"|"rest"
    planned_distance_km FLOAT64,
    planned_duration_min INT64,
    planned_pace_range STRING,
    planned_hr_zone STRING,
    workout_description STRING,
    rationale STRING
  >>,

  status STRING,                          -- "active"|"completed"|"superseded"
  _dp_updated_at TIMESTAMP
)
PARTITION BY week_start_date
CLUSTER BY user_id, status;
```

### Table 4 : `ai_coach__weekly_reviews` ⭐ NOUVEAU
Stocke les compte-rendus hebdomadaires générés automatiquement.

```sql
CREATE TABLE dp_product_dev.ai_coach__weekly_reviews (
  review_id STRING NOT NULL,
  user_id STRING NOT NULL,
  cycle_id STRING NOT NULL,
  week_start_date DATE NOT NULL,
  week_end_date DATE NOT NULL,
  week_number_in_cycle INT64,
  created_at TIMESTAMP NOT NULL,

  -- Plan comparé
  plan_id STRING,                        -- FK vers weekly_plans

  -- Analyse activités (résumé)
  total_planned_km FLOAT64,
  total_actual_km FLOAT64,
  compliance_pct FLOAT64,
  sessions_completed INT64,
  sessions_planned INT64,

  -- Métriques santé moyennes semaine
  avg_sleep_score FLOAT64,
  avg_sleep_duration_hours FLOAT64,
  avg_hrv FLOAT64,
  avg_body_battery_recovery FLOAT64,
  avg_resting_hr INT64,

  -- Review complète (Markdown) ⭐
  review_markdown STRING,                -- Le .md complet généré par Claude
  gcs_markdown_path STRING,              -- gs://bucket/.../reviews/week_XX_review.md

  -- Métadonnées AI
  generated_by STRING,
  generation_prompt_tokens INT64,
  generation_completion_tokens INT64,

  _dp_updated_at TIMESTAMP
)
PARTITION BY week_start_date
CLUSTER BY user_id, cycle_id;
```

### Table 5 : `ai_coach__plan_execution`
Tracking prévu vs réalisé (post-MVP mais utile pour architecture).

```sql
CREATE TABLE dp_product_dev.ai_coach__plan_execution (
  execution_id STRING NOT NULL,
  plan_id STRING NOT NULL,
  user_id STRING NOT NULL,
  date DATE NOT NULL,

  -- Prévu (dénormalisé)
  planned_workout_type STRING,
  planned_distance_km FLOAT64,
  planned_duration_min INT64,

  -- Réalisé (lien vers activités Garmin)
  activity_id INT64,                      -- FK vers hub_health__svc_activities
  actual_distance_km FLOAT64,
  actual_duration_min INT64,
  actual_avg_hr INT64,

  -- Compliance
  compliance_status STRING,               -- "completed"|"partial"|"missed"|"rest_day"
  distance_compliance_pct FLOAT64,

  user_notes STRING,
  _dp_updated_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY user_id, plan_id;
```

### Table 6 : `ai_coach__context_documents`
Métadonnées des contextes utilisateur stockés dans GCS.

```sql
CREATE TABLE dp_product_dev.ai_coach__context_documents (
  context_id STRING NOT NULL,
  user_id STRING NOT NULL,
  uploaded_at TIMESTAMP NOT NULL,

  gcs_path STRING NOT NULL,               -- gs://bucket/user/contexts/uuid.json

  -- Métadonnées du contexte
  context_type STRING,                    -- "race_goal"|"general_training"
  race_date DATE,
  race_distance STRING,                   -- "marathon"|"half_marathon"|"10k"

  -- Résumé (extrait du doc)
  objective STRING,
  constraints ARRAY<STRING>,
  preferences ARRAY<STRING>,

  -- Philosophie d'entraînement ⭐ NOUVEAU
  training_philosophy_json STRING,        -- JSON avec zone distribution, rules, etc.

  -- Usage tracking
  used_in_cycles ARRAY<STRING>,
  used_in_plans ARRAY<STRING>,

  is_active BOOLEAN,
  _dp_updated_at TIMESTAMP
)
CLUSTER BY user_id, is_active;
```

## Structure Google Cloud Storage

**Bucket** : `ela-dataplatform-ai-coach-contexts`

```
gs://ela-dataplatform-ai-coach-contexts/
├── user_etienne/
│   ├── contexts/
│   │   ├── 2026-01-15_marathon_sub_3h30.json
│   │   └── 2026-02-01_base_building.json
│   ├── profiles/                   # Optionnel : historique profils
│   └── generations/                # Optionnel : raw AI responses
└── templates/                      # Templates d'exemple
    ├── marathon_template.json
    └── base_building_template.json
```

**Format Context Document** (JSON uploadé par user) :
```json
{
  "context_id": "uuid-v4",
  "user_id": "user_etienne",
  "created_at": "2026-01-15T10:00:00Z",
  "objective": {
    "type": "race",
    "race_type": "marathon",
    "race_date": "2026-04-20",
    "target_time": "3:30:00",
    "current_level": "intermediate"
  },
  "constraints": {
    "weekly_sessions": 4,
    "max_weekly_volume_km": 80,
    "unavailable_days": ["Sunday morning before 9am"],
    "injury_history": ["IT band syndrome - recovered 2024"],
    "equipment": ["Garmin Forerunner 965", "HRM-Pro strap"]
  },
  "preferences": {
    "training_style": "structured with some flexibility",
    "terrain": "mix of road (70%) and trail (30%)",
    "preferred_workout_types": ["tempo runs", "long runs", "interval sessions"],
    "avoid": ["track workouts", "very early morning runs"],
    "long_run_day": "Saturday",
    "hard_session_day": "Wednesday"
  },
  "training_philosophy": {
    "volume_distribution": {
      "zone_2_pct": 80,
      "zone_3_pct": 10,
      "zone_4_5_pct": 10
    },
    "weekly_structure": {
      "hard_sessions_max": 2,
      "recovery_days_min": 1,
      "long_run_pct_of_weekly_volume": 30
    },
    "progression_rules": {
      "weekly_volume_increase_max_pct": 10,
      "long_run_increase_max_km": 3,
      "consecutive_hard_days_max": 2
    },
    "adaptation_priorities": [
      "Sleep quality first - skip hard session if sleep <7h for 3 days",
      "HRV baseline -10% = recovery week",
      "Body Battery <25 at bedtime = reduce intensity next day"
    ]
  },
  "notes": "Prefer progressive long runs. Need recovery runs to be truly easy (Zone 2 max). Can handle 1-2 hard sessions per week but need full recovery days between."
}
```

## API Endpoints (FastAPI)

**Router** : `/api/routers/ai_coach.py`

### Endpoints MVP Simplifiés (7 endpoints vs 12 initialement)

**Setup / Occasionnel** (Manuel, 1x par cycle) :

1. **POST /api/ai-coach/upload-context**
   - Upload contexte utilisateur vers GCS + store metadata
   - Request : `{"context_type": "race_goal", "context_data": {...}}`
   - Response : `{"context_id": "uuid", "gcs_path": "gs://...", "uploaded_at": "..."}`

2. **POST /api/ai-coach/generate-profile**
   - Génère profil coureur (90 jours d'historique, user hardcodé)
   - Request : `{"analysis_days": 90}` (optionnel, default=90)
   - Response : Profil complet JSON + metadata génération
   - Stocke dans `ai_coach__runner_profiles` avec `is_active=True`

3. **POST /api/ai-coach/generate-cycle**
   - Génère cycle complet 4-12 semaines
   - Request : `{"context_id": "uuid", "profile_id": "uuid", "cycle_start_date": "2026-01-20", "total_weeks": 12}`
   - Response : Cycle complet avec weekly_summaries + cycle_plan_json
   - Stocke dans `ai_coach__training_cycles` + GCS

4. **PUT /api/ai-coach/cycle-config** (Optionnel - facilite édition YAML via API)
   - Met à jour `cycle_config.yaml` dans GCS
   - Request : `{"cycle_id": "...", "start_date": "...", "end_date": "...", "context_id": "...", "profile_id": "..."}`
   - Response : Confirmation + chemin YAML

**Automatisé / Hebdomadaire** (Appelé par Cloud Scheduler) :

5. **POST /api/ai-coach/orchestrate-weekly** ⭐ CŒUR DU SYSTÈME
   - Orchestration complète : review + plan adapté
   - Request : Aucun (lit cycle_config.yaml)
   - Response : `{"review_id": "...", "plan_id": "...", "review_gcs_path": "...", "plan_gcs_path": "..."}`
   - Flow complet : Lit config → Vérifie cycle actif → Génère review → Génère plan → Store BQ + GCS

**Consultation** (Lecture des résultats) :

6. **GET /api/ai-coach/weekly-review/latest**
   - Récupère dernier compte-rendu hebdo (markdown + métriques)
   - Response : Review complet avec lien GCS vers .md

7. **GET /api/ai-coach/plans/current**
   - Récupère plan actif pour semaine en cours (markdown + daily_workouts)
   - Response : Plan complet avec lien GCS vers .md

**Pydantic Models** : `/api/models/ai_coach.py` (pattern de [activities.py](api/models/activities.py))
- `ContextUploadRequest`, `ContextResponse`, `TrainingPhilosophy`
- `RunnerProfile`, `ProfileGenerationRequest`
- `TrainingCycle`, `CycleGenerationRequest`, `WeeklySummary`
- `WeeklyPlan`, `PlanGenerationRequest`, `DailyWorkout`
- `WeeklyReview`, `ReviewMetrics` ⭐
- `CycleConfigUpdate` ⭐
- Utiliser `datetime` de Python standard, pas `Optional` sauf si vraiment nullable

## Stratégie de Données

### Agrégation des Données pour l'IA

**Principe** : Utiliser les tables **existantes** `hub_health__svc_*` et `product pct_*` via **requêtes runtime** (pas de nouveaux modèles dbt pour MVP).

**Tables à Exploiter** :
- `hub_health__svc_activities` : Activités avec GPS, zones HR, charge
- `hub_health__svc_sleep` : Sommeil, HRV, body battery, FC repos
- Lake models : Training readiness, VO2 max, stress

**Optimisation Tokens** (critique pour coûts) :
- **Derniers 7 jours** : Données complètes détaillées (~2K tokens)
- **Jours 8-30** : Résumé par activité, pas de GPS (~1K tokens)
- **Jours 31-90** : Agrégats hebdomadaires uniquement (~500 tokens)
- **Total** : ~8K tokens input (vs 100K sans optimisation)

**Fonctions d'Agrégation** (`data_aggregator.py`) :
```python
def get_activity_summary_for_profile(user_id: str, days: int = 90) -> dict
def get_health_summary_for_profile(user_id: str, days: int = 90) -> dict
def get_recent_health_snapshot(user_id: str, days: int = 7) -> dict
def get_recent_activity_snapshot(user_id: str, days: int = 7) -> dict
```

## Intégration Anthropic Claude

### Setup SDK

**Dépendance** : `anthropic>=0.18.0` (à ajouter dans `pyproject.toml`)

**Client Wrapper** (`anthropic_client.py`) :
```python
class ClaudeClient:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model = "claude-opus-4-5-20251101"

    def generate(self, system_prompt: str, user_prompt: str,
                 max_tokens: int = 4096) -> dict:
        # Appel API avec tracking tokens/coûts
        # Retourne: {"content": "...", "usage": {...}, "model": "..."}
```

### Stratégie de Prompting

**Principes** :
1. **Structured Output** : Imposer JSON schema strict dans system prompt
2. **Token Optimization** : Sampling intelligent des données
3. **Domain Expertise** : Injecter connaissances physiologie running
4. **Context Layering** : Health → Activity → User Context → Request

**Exemple System Prompt (Profil)** :
```
You are an expert running coach with 20+ years of experience analyzing wearable data.

Task: Create a comprehensive runner profile from provided data.

OUTPUT FORMAT (strict JSON):
{
  "runner_level": "beginner|intermediate|advanced|elite",
  "weekly_volume_km": float,
  "vo2_max_estimate": float,
  "primary_strengths": ["endurance", "consistency"],
  "primary_weaknesses": ["speed_work", "recovery"],
  "training_zones": {...},
  "analysis": {...},
  "recommendations": {...}
}

DATA INTERPRETATION:
- Sleep score >80 = good recovery
- HRV 40-60ms = typical trained runner
- Body Battery <25 at bedtime = overtraining risk
- Zone 2: 70-80% weekly volume ideal
```

**Coûts Estimés** (Claude Opus 4.5 : $15/M input, $75/M output) :
- Génération profil : ~8K input + 3K output = **$0.40**
- Plan hebdo : ~5K input + 2K output = **$0.25**
- **Mensuel** (1 profil + 4 plans) : **~$1.60/user**

## Plan d'Implémentation (Phases)

### Phase 0 : Foundation (Pré-requis)

**Objectif** : Infrastructure & dépendances

**Tâches** :
1. Ajouter `anthropic>=0.18.0` dans [pyproject.toml:19](pyproject.toml)
2. Créer les 5 tables BigQuery (DDL ci-dessus)
3. Créer bucket GCS `ela-dataplatform-ai-coach-contexts` avec IAM approprié
4. Ajouter `ANTHROPIC_API_KEY` dans `.env` local et GCP Secret Manager

**Livrable** : Infrastructure prête

---

### Phase 1 : Service Layer - Data Aggregation

**Objectif** : Couche de récupération des données depuis BigQuery

**Fichiers à Créer** :
- `/src/services/ai_coach/__init__.py`
- `/src/services/ai_coach/config.py`
- `/src/services/ai_coach/data_aggregator.py`

**Implémentation** :
- Fonctions d'agrégation intelligente (sampling 7/30/90 jours)
- Requêtes vers `hub_health__svc_activities`, `hub_health__svc_sleep`
- Tests unitaires avec mock BigQuery

**Pattern** : Suivre style existant dans `/src/connectors/fetcher/`

**Livrable** : Agrégation données testée, <10K tokens par requête

---

### Phase 2 : Service Layer - AI Integration (Profil)

**Objectif** : Génération profil coureur (Use Case #1)

**Fichiers à Créer** :
- `/src/services/ai_coach/anthropic_client.py`
- `/src/services/ai_coach/prompt_builder.py`
- `/src/services/ai_coach/profile_generator.py`
- `/src/services/ai_coach/response_parser.py`

**Implémentation** :
1. **anthropic_client.py** : Wrapper Claude API avec tracking tokens
2. **prompt_builder.py** :
   - `build_profile_system_prompt()` : Instructions + JSON schema
   - `build_profile_user_prompt(data)` : Injection données santé/activités
3. **profile_generator.py** :
   - `generate_runner_profile(user_id, days=90)` : Orchestration complète
   - Flow : Aggregate data → Build prompt → Call Claude → Parse → Store BQ
4. **response_parser.py** : Validation JSON, extraction champs structurés

**Stockage** : Insert dans `ai_coach__runner_profiles`, set `is_active=True`

**Livrable** : Génération profil fonctionnelle (test local)

---

### Phase 3 : API Layer - Profile Endpoints

**Objectif** : Exposer génération profil via API REST

**Fichiers à Créer** :
- `/api/routers/ai_coach.py`
- `/api/models/ai_coach.py`

**Implémentation** :
1. Créer router FastAPI (pattern similaire à [api/routers/homepage.py](api/routers/homepage.py))
2. Pydantic models : `RunnerProfile`, `ProfileGenerationRequest`
3. Endpoints :
   - `POST /api/ai-coach/generate-profile`
   - `GET /api/ai-coach/profile/latest`
4. Enregistrer router dans [api/main.py:44](api/main.py)

**Tests** : `uvicorn api.main:app --reload` + Postman/curl

**Livrable** : API profil accessible, réponses validées

---

### Phase 4 : Service Layer - Context Management

**Objectif** : Upload/stockage contextes utilisateur

**Fichiers à Créer** :
- `/src/services/ai_coach/gcs_manager.py`

**Implémentation** :
- `upload_context(user_id, context_data)` : Upload GCS + insert metadata BQ
- `get_context(context_id)` : Retrieve from GCS
- Pattern : S'inspirer de `/src/connectors/fetcher/gcs_writer.py`

**API Endpoint** :
- `POST /api/ai-coach/upload-context` dans router existant
- `GET /api/ai-coach/contexts/{context_id}`

**Livrable** : Upload/retrieval contextes fonctionnel

---

### Phase 5 : Service Layer - Plan Generation (Cycle + Weekly)

**Objectif** : Génération cycle complet ET plans hebdo (fusionné)

**Fichiers à Créer** :
- `/src/services/ai_coach/plan_generator.py`

**Implémentation** :
1. **prompt_builder.py** (extension) :
   - `build_cycle_system_prompt()` : Instructions plan complet multi-semaines
   - `build_cycle_user_prompt(profile, context, health_snapshot)`
   - `build_plan_system_prompt()` : Instructions plan 7 jours avec contexte cycle
   - `build_plan_user_prompt(cycle, profile, context, health_snapshot, week_number)`

2. **plan_generator.py** (2 fonctions principales) :
   - `generate_training_cycle(context_id, profile_id, cycle_start_date, total_weeks)`
     - Flow : Get context (GCS) → Get profile (BQ) → Get recent health → Build prompt → Claude → Parse → Store BQ + GCS
   - `generate_weekly_plan(cycle_id, week_start_date, regeneration_reason)`
     - Flow : Get cycle (BQ) → Get context (GCS) → Get profile (BQ) → Get recent health → Build prompt → Claude → Parse → Store BQ + GCS
     - Génère aussi le .md du plan

3. **response_parser.py** (extension) :
   - Parser cycle complet + weekly_summaries
   - Parser plan hebdo + daily_workouts

**Stockage** :
- Cycle : Insert dans `ai_coach__training_cycles` + GCS JSON
- Plan : Insert dans `ai_coach__weekly_plans` + GCS .md

**Livrable** : Génération cycle + plans testée localement

---

### Phase 6 : Service Layer - Weekly Reviewer + Orchestrator ⭐

**Objectif** : Cœur de l'automatisation hebdomadaire

**Fichiers à Créer** :
- `/src/services/ai_coach/weekly_reviewer.py`
- `/src/services/ai_coach/orchestrator.py`

**Implémentation** :

1. **weekly_reviewer.py** :
   - `generate_weekly_review(plan_id, week_start_date, week_end_date)`
   - Flow :
     - Get plan semaine écoulée (BQ)
     - Get activités réelles (BQ `hub_health__svc_activities`)
     - Get santé/sommeil 7j (BQ `hub_health__svc_sleep`, etc.)
     - Build prompt comparaison prévu vs réalisé
     - Claude → Génère review .md complet
     - Parse métriques clés (compliance, avg HRV, etc.)
     - Store dans `ai_coach__weekly_reviews` + GCS .md

2. **prompt_builder.py** (extension) :
   - `build_review_system_prompt()` : Instructions analyse complète semaine
   - `build_review_user_prompt(planned, actual, health)` : Données structurées

3. **orchestrator.py** (fonction principale) :
   - `orchestrate_weekly()`
   - Flow complet :
     1. Load `cycle_config.yaml` from GCS
     2. Check if today in `[start_date, end_date]` → exit if not
     3. Calculate week_number from cycle start
     4. Call `weekly_reviewer.generate_weekly_review()` (semaine écoulée)
     5. Call `plan_generator.generate_weekly_plan()` (semaine suivante, adapté au review)
     6. Return review_id + plan_id + GCS paths

**Livrable** : Orchestration complète testée localement

---

### Phase 7 : API Layer - Endpoints Simplifiés

**Objectif** : Exposer 7 endpoints (vs 12 initialement)

**Implémentation** :
- 7 endpoints définis dans section API Endpoints
- Pattern : Suivre [homepage.py](api/routers/homepage.py) (try/except HTTPException, async, get_bq_client())
- Pydantic models : Toutes les classes définies section API Endpoints
- Enregistrer router dans [main.py](api/main.py) : `app.include_router(ai_coach.router, prefix="/api/ai-coach", tags=["ai-coach"])`

**Endpoints clés** :
- Setup : upload-context, generate-profile, generate-cycle, cycle-config
- Auto : orchestrate-weekly ⭐
- Consultation : weekly-review/latest, plans/current

**Tests End-to-End** :
1. Upload context → verify GCS + BQ
2. Generate profile → verify BQ + is_active
3. Generate cycle → verify BQ + GCS JSON
4. Update cycle_config.yaml (manuel ou API)
5. Call orchestrate-weekly → verify review + plan générés
6. GET review/latest et plans/current → verify .md accessibles

**Livrable** : MVP complet (Setup + Orchestration) fonctionnel en local

---

### Phase 8 : Tests & Validation

**Objectif** : Couverture tests complète (unitaire + intégration)

**Fichiers à Créer** :
- `/tests/services/ai_coach/test_data_aggregator.py`
- `/tests/services/ai_coach/test_anthropic_client.py`
- `/tests/services/ai_coach/test_response_parser.py`
- `/tests/api/test_ai_coach_router.py`

**Implémentation** :

1. **Tests Unitaires Service Layer** :
   - `test_data_aggregator.py` : Mock BigQuery, valider requêtes + sampling
   - `test_response_parser.py` : Valider parsing JSON Claude (profil, cycle, plan)
   - Mock responses Claude pour tester sans consommer API

2. **Tests d'Intégration API** :
   - `test_ai_coach_router.py` : FastAPI TestClient
   - Mock service layer pour tester endpoints isolément
   - Valider request/response schemas Pydantic

3. **Test E2E avec Mock Claude** :
   - Mocker `anthropic.Client` pour retourner réponses prédéfinies
   - Valider flow complet : context → profile → cycle → plan

**Livrable** : Coverage >80% sur service layer, tous endpoints testés

---

### Phase 9 : Template Context & Documentation

**Objectif** : Créer template exemple + upload dans GCS

**Fichiers à Créer** :
- `/templates/marathon_context_template.json` (local, puis upload GCS)
- Optionnel : `/docs/ai_coach_usage.md` (guide utilisateur)

**Template Marathon** :
```json
{
  "context_id": "example-marathon-sub-3h30",
  "user_id": "user_etienne",
  "created_at": "2026-01-15T10:00:00Z",
  "objective": {
    "type": "race",
    "race_type": "marathon",
    "race_date": "2026-04-20",
    "target_time": "3:30:00",
    "current_level": "intermediate",
    "current_weekly_volume_km": 45
  },
  "constraints": {
    "weekly_sessions": 4,
    "max_weekly_volume_km": 80,
    "unavailable_days": ["Sunday morning before 9am"],
    "injury_history": ["IT band syndrome - recovered 2024"],
    "equipment": ["Garmin Forerunner 965", "HRM-Pro strap"]
  },
  "preferences": {
    "training_style": "structured with some flexibility",
    "terrain": "mix of road (70%) and trail (30%)",
    "preferred_workout_types": ["tempo runs", "long runs", "interval sessions"],
    "avoid": ["track workouts", "very early morning runs"],
    "long_run_day": "Saturday",
    "hard_session_day": "Wednesday"
  },
  "notes": "Prefer progressive long runs. Need recovery runs to be truly easy (Zone 2 max). Can handle 1-2 hard sessions per week but need full recovery days between."
}
```

**Actions** :
1. Créer template local
2. Utiliser endpoint `POST /api/ai-coach/upload-context` pour uploader
3. Vérifier présence dans GCS `templates/marathon_template.json`

**Livrable** : Template prêt à l'emploi, documenté

---

### Phase 10 : Deployment & Cloud Scheduler CRON ⭐

**Objectif** : Déployer en production + automatisation hebdo

**Tâches** :

1. **Build & Deploy API** :
   - Vérifier [Dockerfile:22](Dockerfile) inclut Anthropic SDK (`anthropic>=0.18.0`)
   - Ajouter `ANTHROPIC_API_KEY` dans GCP Secret Manager
   - Mettre à jour Cloud Run service `ela-api` pour injecter secret
   - Déployer via CI/CD existant (merge dans `main`)

2. **Configuration Cloud Scheduler** (nouveau CRON job) :
   ```bash
   gcloud scheduler jobs create http ai-coach-weekly-orchestrator \
     --location=us-central1 \
     --schedule="0 21 * * 0" \
     --time-zone="Europe/Paris" \
     --uri="https://ela-api-xxx.run.app/api/ai-coach/orchestrate-weekly" \
     --http-method=POST \
     --oidc-service-account-email="cloud-scheduler@PROJECT_ID.iam.gserviceaccount.com" \
     --oidc-token-audience="https://ela-api-xxx.run.app"
   ```
   - Schedule : `0 21 * * 0` = Dimanche 21h (Europe/Paris)
   - Authentifié via OIDC (Cloud Run service account)

3. **Permissions IAM** :
   - Accorder `roles/run.invoker` à service account du scheduler
   - Accorder `roles/storage.objectAdmin` à Cloud Run pour GCS access

4. **Cycle Config Initial** :
   - Upload `cycle_config.yaml` dans GCS (manuellement ou via API)
   - Vérifier structure correcte

5. **Monitoring & Alertes** :
   - Cloud Logging : Filtrer logs `orchestrate-weekly`
   - Billing alerts : <$50/mois Anthropic API
   - Cloud Monitoring : Dashboard pour latency/errors

**Fichiers** : Aucune modification CI/CD nécessaire ([.github/workflows/cd-deploy-api.yaml](https://github.com/anthropics/claude-code/blob/main/.github/workflows/cd-deploy-api.yaml) déjà configuré)

**Livrable** : AI Coach live en production avec CRON hebdo actif

---

### Phase 11+ : Post-MVP (Optionnel)

**Features futures** :
- **Tracking prévu vs réalisé automatique** : Lier automatiquement activités Garmin aux daily_workouts, calculer compliance
- **Adaptation dynamique en cours de semaine** : Ajuster plan si HRV/sommeil dégradés (ex: skip hard session si body battery <25)
- **Conversational interface** : Chat avec historique pour poser questions au coach ("Pourquoi cette séance ?", "Puis-je faire plus ?")
- **Multi-utilisateurs** : Ajouter JWT auth, user_id dynamique, migration tables
- **Notifications** : Email/push hebdomadaire avec plan généré automatiquement (Cloud Scheduler + SendGrid)
- **Visualisations** : Dashboard avec progression profil dans le temps, analyse tendances

## Décisions Techniques Critiques

### 1. Approche Hybride Cycle + Hebdo

**Implémentation** :
- Générer cycle complet 8-12 semaines (stocké dans `ai_coach__training_cycles`)
- Régénérer plan hebdo chaque semaine basé sur :
  - Plan cycle (référence)
  - Exécution semaine précédente
  - Métriques santé actuelles
  - Feedback utilisateur

**Bénéfice** : Structure long-terme + adaptation réaliste

### 2. Versioning Profils & Contextes

**Stratégie** : Versioning temporel avec flag `is_active`
- Un seul profil actif par user
- Historique conservé pour analyse d'évolution
- Contextes immuables (nouveau upload = nouveau context_id)

### 3. Gestion Erreurs AI

**Robustesse** :
- Timeout Claude : 30s max
- Retry : 3 tentatives avec backoff exponentiel
- Validation JSON stricte (reject si malformed)
- Fallback : HTTP 503 si échec AI
- Logging : Tous prompts/responses dans GCS pour debug

### 4. Optimisation Coûts

**Stratégies** :
- Sampling intelligent (section Data Aggregation)
- Rate limiting : 1 profil/jour, 1 plan/semaine par user
- max_tokens=4096 pour cap output
- Monitoring usage dans BQ
- Billing alerts à $50/mois

**Coût estimé** : $1.40/mois/user (acceptable MVP)

## Risques & Mitigations

### Risque 1 : Consistance Prompts
- **Problème** : Claude peut générer formats inconsistants
- **Mitigation** : Tests extensifs, exemples dans prompts, validation stricte

### Risque 2 : Performance BigQuery
- **Problème** : Requêtes runtime lentes (>5s)
- **Mitigation** : Utiliser product layer, clustering, optionnel dbt materialized views si besoin

### Risque 3 : Sécurité Plans
- **Problème** : IA peut générer plans dangereux (surcharge, blessures)
- **Mitigation** :
  - Règles sécurité dans prompts (max +10% volume/semaine)
  - Validation post-génération (code checks)
  - Plans = suggestions, pas prescriptions médicales

**Exemple validation** :
```python
def validate_plan_safety(plan):
    if plan.weekly_km > previous_week * 1.1:
        warnings.append("Volume increase >10%, injury risk")
```

## Fichiers Critiques à Créer/Modifier

### Créer (Nouveaux)
- `/src/services/ai_coach/` : 8 modules Python (service layer)
- `/api/routers/ai_coach.py` : Router FastAPI
- `/api/models/ai_coach.py` : Pydantic models
- 5 tables BigQuery (DDL SQL)
- Bucket GCS `ela-dataplatform-ai-coach-contexts`

### Modifier (Existants)
- [pyproject.toml:19](pyproject.toml) : Ajouter `anthropic>=0.18.0`
- [api/main.py:44](api/main.py) : Register nouveau router
- `.env` : Ajouter `ANTHROPIC_API_KEY`
- GCP Secret Manager : Ajouter secret API key

### Exploiter (Existants, pas de modif)
- [src/dbt_dataplatform/models/hub/health/svc_activities.sql](src/dbt_dataplatform/models/hub/health/svc_activities.sql) : Source activités
- [src/dbt_dataplatform/models/hub/health/svc_sleep.sql](src/dbt_dataplatform/models/hub/health/svc_sleep.sql) : Source santé
- [api/routers/homepage.py](api/routers/homepage.py) : Pattern de référence
- [src/connectors/fetcher/gcs_writer.py](src/connectors/fetcher/gcs_writer.py) : Pattern GCS

## Observabilité

**Logging** :
- Tous appels AI → GCS `gs://ela-dataplatform-logs/ai-coach/`
- Include : prompt hash, tokens, latency, cost

**Métriques** (Cloud Monitoring) :
- Latency AI requests (p50/p95/p99)
- Token usage par requête
- Coûts journaliers
- Taux d'erreur

**Alertes** :
- Coût journalier > $5
- Error rate > 10%
- Latency p95 > 30s

## Résumé : Leverage vs Build

### ✅ Exploiter l'Existant
- Données : `hub_health__svc_*` tables (28+ métriques Garmin)
- API pattern : FastAPI + Pydantic + BigQuery client
- Deployment : Cloud Run + CI/CD existants
- Infrastructure : GCS, BigQuery, IAM

### 🆕 Construire du Neuf
- Service AI : 8 modules Python (`/src/services/ai_coach/`)
- Tables BQ : 5 nouvelles tables
- Bucket GCS : contextes utilisateur
- API router : `/api/routers/ai_coach.py`
- SDK Anthropic : Intégration Claude Opus 4.5

### 🔧 Modifier
- Dependencies : `anthropic>=0.18.0`
- Environment : `ANTHROPIC_API_KEY`
- API main : Register router

---

## Résumé Estimation

**Scope MVP** : Phases 0-10

### Use Cases Couverts
✅ **Setup Initial** (Manuel, 1x par cycle) :
- Génération profil coureur (90j historique)
- Génération cycle complet (4-12 semaines)
- Configuration cycle via YAML

✅ **Automatisation Hebdomadaire** (CRON Dimanche 21h) :
- Weekly Review (.md) : Analyse complète prévu vs réalisé + santé
- Weekly Plan Adapté (.md) : Plan semaine suivante basé sur review + philosophie

✅ **Infrastructure** :
- Stockage hybride (BigQuery analytics + GCS .md lisibles)
- Orchestration automatique via Cloud Scheduler
- Tests complets + template + deployment

**Estimation Temporelle** :
- Phases 0-3 (Foundation + Data + AI Profil) : ~2 semaines
- Phases 4-5 (Context + Plan Generation) : ~2 semaines
- Phase 6 (Weekly Reviewer + Orchestrator) ⭐ : ~2 semaines
- Phases 7-9 (API + Tests + Template) : ~1.5 semaines
- Phase 10 (Deployment + Cloud Scheduler) : ~0.5 semaine
- **Total MVP** : ~8 semaines

**Coût Opérationnel** :
- Génération profil : $0.40 (occasionnel, 1x par cycle)
- Génération cycle : $0.50 (occasionnel, 1x par cycle)
- **Weekly review** : $0.30/semaine × 4 = **$1.20/mois** ⭐
- **Weekly plan** : $0.25/semaine × 4 = **$1.00/mois** ⭐
- **Total hebdo** : ~$2.20/mois (orchestration automatique)

**Complexité** : Moyenne-élevée
- AI integration (nouveau)
- Data engineering (exploite existant)
- 10 nouveaux modules Python (dont orchestrator + reviewer)
- 6 nouvelles tables BigQuery
- 7 endpoints API (simplifiés vs 12 initialement)
- Cloud Scheduler CRON
- Cycle Config YAML (source of truth)
