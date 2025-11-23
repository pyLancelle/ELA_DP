# Analyse Spécifique : Garmin Fetch (`garmin_fetch.py`)

## 1. Diagnostic : Pourquoi est-ce un "Monolithe" ?

Le fichier `src/connectors/garmin/garmin_fetch.py` fait **1447 lignes**. C'est effectivement trop pour un seul script, mais le problème n'est pas seulement la taille, c'est la **répétition**.

### Problèmes Identifiés :
1.  **Violation du principe DRY (Don't Repeat Yourself) :**
    Il y a plus de **30 fonctions** (`fetch_sleep_data`, `fetch_steps_data`, `fetch_heart_rate_data`, etc.) qui font exactement la même chose :
    *   Calculer une plage de dates.
    *   Boucler sur chaque jour.
    *   Appeler une méthode spécifique du client `Garmin`.
    *   Gérer les erreurs (try/except).
    *   Formatter le résultat (ajouter la date).
    *   Attendre (sleep) pour le rate-limiting.
    *   Retourner une liste.

    *Exemple de duplication :*
    ```python
    # fetch_sleep_data
    try:
        data = client.get_sleep_data(date_str)
        # ... gestion erreur, sleep ...
    except: ...

    # fetch_steps_data
    try:
        data = client.get_steps_data(date_str)
        # ... gestion erreur, sleep ...
    except: ...
    ```
    C'est du "copier-coller" structurel. Si vous voulez changer le temps de pause ou la gestion des logs, vous devez modifier 30 endroits.

2.  **Mélange des Responsabilités :**
    Le fichier gère tout :
    *   Parsing des arguments CLI (`argparse`).
    *   Configuration du Logging.
    *   Logique de synchronisation Withings (métier).
    *   Logique d'appel API (infra).
    *   Écriture des fichiers JSONL (IO).

3.  **Testabilité Nulle :**
    Il est impossible de tester unitairement la logique de récupération sans mocker tout le client Garmin et le système de fichiers.

---

## 2. Plan de Refactoring Recommandé

L'objectif est de passer d'un script procédural géant à une architecture modulaire et configurable (comme Spotify v2).

### Étape 1 : Créer un "Generic Fetcher"
Au lieu d'avoir 30 fonctions, on peut en avoir **une seule** générique qui prend en paramètre la méthode à appeler.

**Concept (Pseudo-code) :**
```python
class GarminFetcher:
    def fetch_daily_metric(self, metric_name: str, method_name: str, start_date, end_date):
        """Récupère une métrique journalière générique"""
        results = []
        for date in date_range(start_date, end_date):
            try:
                # Appel dynamique de la méthode du client
                method = getattr(self.client, method_name)
                data = method(date.strftime("%Y-%m-%d"))
                
                if data:
                    results.append({"date": date, "data": data, "type": metric_name})
                
                time.sleep(0.3) # Rate limiting centralisé
            except Exception as e:
                logging.warning(f"Error fetching {metric_name}: {e}")
        return results
```

### Étape 2 : Externaliser la Configuration
Définir les métriques à récupérer dans une configuration (dictionnaire ou YAML), plutôt que dans le code.

```python
# config.py
METRICS_CONFIG = {
    "sleep": {"method": "get_sleep_data", "type": "daily"},
    "steps": {"method": "get_steps_data", "type": "daily"},
    "activities": {"method": "get_activities_by_date", "type": "range"}, # Certaines méthodes prennent une plage
}
```

### Étape 3 : Découpage en Modules
Restructurer le dossier `src/connectors/garmin/` :

```text
src/connectors/garmin/
├── __init__.py
├── main.py            # Point d'entrée CLI (ancien garmin_fetch.py, mais vide)
├── client.py          # Gestion de l'auth et instance Garmin
├── fetcher.py         # Logique générique de fetch (la classe GarminFetcher)
├── config.py          # Définition des métriques (METRICS_CONFIG)
├── utils.py           # Helpers (dates, jsonl writer)
└── services/
    └── withings.py    # Logique de sync Withings isolée
```

## 3. Bénéfices Attendus

1.  **Réduction de Code :** On passera de ~1500 lignes à probablement ~300-400 lignes.
2.  **Maintenance Facile :** Ajouter une nouvelle métrique se fera en ajoutant une ligne dans `config.py`, sans toucher au code.
3.  **Robustesse :** La gestion des erreurs et le rate-limiting seront centralisés. Si l'API change, on corrige à un seul endroit.
4.  **Testabilité :** On pourra tester `GarminFetcher` avec un faux client (mock) pour vérifier qu'il gère bien les erreurs et les boucles.

## Conclusion
Votre intuition est bonne. Ce fichier est un candidat parfait pour du refactoring. Ce n'est pas "grave" en soi (ça marche), mais c'est une dette technique qui ralentira les évolutions futures.
