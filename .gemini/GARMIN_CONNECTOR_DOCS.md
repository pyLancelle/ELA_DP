# Documentation Technique : Connecteur Garmin Modulaire

Ce document détaille le fonctionnement interne du connecteur Garmin refactorisé (`src/connectors/garmin/`). Il est destiné aux développeurs souhaitant comprendre, maintenir ou étendre ce module.

## 1. Architecture Globale

L'ancien script monolithique a été découpé en composants spécialisés suivant le principe de séparation des responsabilités.

```text
src/connectors/garmin/
├── __main__.py       # 🎮 Point d'entrée (CLI & Orchestration)
├── config.py         # ⚙️ Configuration (Définition des métriques)
├── fetcher.py        # 🧠 Logique de récupération (Generic Fetcher)
├── client.py         # 🔐 Authentification & Session
└── utils.py          # 🛠️ Utilitaires (I/O, Logs, Dates)
```

---

## 2. Détail des Composants

### A. `config.py` & `metrics.yaml` : Le Cerveau
C'est le fichier le plus important pour la maintenance courante. La configuration est désormais externalisée dans `metrics.yaml`.

`config.py` se charge de charger ce fichier YAML.
`metrics.yaml` contient la définition des métriques :

```yaml
sleep:
  method: get_sleep_data  # Nom de la méthode dans la lib garminconnect
  type: daily             # Stratégie de récupération (voir Fetcher)
  description: Sleep tracking data
```
*Pour ajouter une nouvelle métrique, c'est ici que ça se passe.*

### B. `fetcher.py` : Le Moteur
La classe `GarminFetcher` est agnostique des données. Elle ne connaît pas "le sommeil" ou "les pas", elle connaît des **stratégies de récupération**.

Elle expose une méthode principale : `fetch_metric(metric_name, start_date, end_date)`.
En fonction du `type` défini dans la config, elle délègue à une méthode interne :

1.  **`daily`** : Boucle jour par jour (ex: sommeil, pas).
2.  **`range`** : Appelle l'API avec une date de début et de fin (ex: body composition).
3.  **`simple`** : Appelle l'API sans paramètres (ex: liste des devices).
4.  **`activity_details`** : Stratégie complexe en 2 temps (liste des activités -> détail pour chaque ID).
5.  **`activity_subdata`** : Pour les sous-données d'activité (météo, splits, etc.).

C'est ici qu'est centralisée la gestion des erreurs et le `time.sleep()` pour le rate-limiting.

### C. `__main__.py` : L'Orchestrateur
C'est le script exécuté par la ligne de commande.
1.  Il parse les arguments (`--days`, `--data-types`, etc.).
2.  Il charge les variables d'environnement via `client.py`.
3.  Il gère la synchronisation Withings (si activée).
4.  Il instancie le `GarminFetcher`.
5.  Il boucle sur les types de données demandés et sauvegarde les résultats via `utils.write_jsonl`.

### D. `client.py` : L'Accès
Wrapper autour de la librairie `garminconnect`. Il s'assure que l'on est bien authentifié avant de renvoyer l'instance du client.

---

## 3. Flux d'Exécution

1.  **Lancement** : `python -m src.connectors.garmin --days 1`
2.  **Init** : `__main__` charge `.env` et crée `GarminClient`.
3.  **Auth** : `GarminClient` se connecte à Garmin Connect.
4.  **Boucle** : Pour chaque métrique (par défaut toutes celles de `config.py`) :
    *   `__main__` appelle `fetcher.fetch_metric("sleep", ...)`
    *   `fetcher` regarde `config.py` -> type "daily".
    *   `fetcher` exécute la boucle `_fetch_daily`.
    *   Pour chaque jour, appel de `client.get_sleep_data()`.
    *   `fetcher` retourne une liste de dictionnaires normalisés (ajout de `date` et `data_type`).
5.  **Sauvegarde** : `__main__` appelle `utils.write_jsonl` pour écrire le fichier sur le disque.

---

## 4. Guide d'Extension

### Cas 1 : Ajouter une métrique simple (ex: "Hydratation")
1.  Vérifiez si la méthode existe dans la librairie `garminconnect` (ex: `get_hydration_data`).
2.  Ouvrez `src/connectors/garmin/metrics.yaml`.
3.  Ajoutez l'entrée :
    ```yaml
    hydration:
        method: get_hydration_data
        type: daily # Si c'est une donnée par jour
        description: Water intake
    ```
4.  C'est tout ! Le script la prendra en compte automatiquement.

### Cas 2 : Ajouter une stratégie complexe
Si l'API Garmin demande une logique bizarre (ex: appel A, puis appel B avec le résultat de A) :
1.  Ajoutez un nouveau `type` dans `config.py` (ex: `"complex_stuff"`).
2.  Dans `src/connectors/garmin/fetcher.py`, modifiez `fetch_metric` pour gérer ce nouveau type.
3.  Implémentez la méthode `_fetch_complex_stuff(...)` dans `GarminFetcher`.

---

## 5. Tests

Le module est couvert par deux types de tests :
1.  **Tests Unitaires** (`tests/connectors/garmin/test_refactored.py`) : Vérifient que la mécanique interne (le fetcher) appelle bien les bonnes méthodes du client mocké.
2.  **Tests de Régression** (supprimés après validation, mais ré-créables) : Comparent la sortie JSONL avec une version de référence.

Pour lancer les tests :
```bash
uv run pytest tests/connectors/garmin/
```
