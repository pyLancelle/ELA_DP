# Mode Opératoire : Transformation des Nested Arrays dans Garmin Fetcher

## 🎯 Objectif
Transformer automatiquement les nested arrays (`[[a,b]]`) en objets (`[{x:a, y:b}]`) directement dans le connector Garmin pour garantir la compatibilité BigQuery native.

---

## 📊 Analyse de la Situation

### Fichiers concernés (identifiés lors des tests)
```
body_battery       → bodyBatteryValuesArray: [[timestamp, value]]
heart_rate         → heartRateValues: [[timestamp, value]]
stress             → stressValuesArray: [[timestamp, "MEASURED", value, score]]
stress             → bodyBatteryValuesArray: [[timestamp, value]]
floors             → floorValuesArray: [[timestamp, value]]
intensity_minutes  → (champ inconnu): [[timestamp, value]]
respiration        → respirationValuesArray: [[timestamp, value]]
respiration        → respirationAveragesValuesArray: [[ts, avg, high, low]]
spo2               → (champ inconnu): [[timestamp, value]]
```

### Patterns observés
1. **Cas général (80%)** : `[[timestamp, value]]` → 2 éléments
2. **Cas spéciaux** : 
   - `stressValuesArray`: 4 éléments `[timestamp, type, value, score]`
   - `respirationAveragesValuesArray`: 4 éléments `[timestamp, average, high, low]`

### Stratégie de détection
- ✅ **Détection structurelle** : Parcourir récursivement tout le JSON
- ✅ **Cas spéciaux** : Dictionnaire de mappings explicites
- ✅ **Fallback générique** : Si 2 éléments → `{timestamp, value}`
- ⚠️ **Alerte** : Si >2 éléments et pas de mapping → logger un warning

---

## 🏗️ Architecture de la Solution

### 1. Créer la fonction de transformation dans `utils.py`

**Fichier** : `src/connectors/garmin/utils.py`

La fonction doit :
- Être **récursive** pour gérer les nested objects arbitraires
- Utiliser un **dictionnaire de mappings** pour les cas connus
- Avoir un **fallback générique** pour les cas simples
- **Logger les warnings** pour les nested arrays non mappés

### 2. Appliquer la transformation dans `fetcher.py`

**Point d'injection** : Juste avant l'écriture du JSONL

Modifier ces méthodes :
- `_fetch_daily()` : ligne 91-93
- `_fetch_range()` : ligne 138-142
- `_fetch_simple()` : ligne 164-170
- `_fetch_activity_details()` : ligne 202-207
- `_fetch_activity_subdata()` : ligne 262, 272-275

### 3. Tests de validation

- Ingérer tous les fichiers de test `2025_11_22_*`
- Vérifier que BigQuery accepte tous les fichiers
- Comparer les schémas auto-détectés

---

## 💻 Implémentation Étape par Étape

### ÉTAPE 1 : Ajouter la fonction de transformation dans `utils.py`

**Code à ajouter** à la fin de `src/connectors/garmin/utils.py` :

```python
def flatten_nested_arrays(
    obj: Any, 
    known_mappings: Dict[str, List[str]] = None,
    path: str = ""
) -> Any:
    """
    Transforme récursivement les nested arrays pour compatibilité BigQuery.
    
    BigQuery ne supporte pas les nested arrays ([[a,b]]).
    Cette fonction les transforme en tableaux d'objets ([{x:a, y:b}]).
    
    Args:
        obj: Objet à transformer (dict, list, ou primitive)
        known_mappings: Mappings explicites pour les cas spéciaux
            Format: {"field_name": ["key1", "key2", ...]}
        path: Chemin actuel dans l'objet (pour logging)
    
    Returns:
        Objet transformé avec nested arrays aplatis
    
    Examples:
        >>> flatten_nested_arrays([[1, 2], [3, 4]])
        [{'timestamp': 1, 'value': 2}, {'timestamp': 3, 'value': 4}]
        
        >>> flatten_nested_arrays(
        ...     {"data": [[100, "MEASURED", 42, 3.0]]},
        ...     {"data": ["timestamp", "type", "value", "score"]}
        ... )
        {'data': [{'timestamp': 100, 'type': 'MEASURED', 'value': 42, 'score': 3.0}]}
    """
    # Mappings par défaut (cas connus de Garmin)
    if known_mappings is None:
        known_mappings = {
            'stressValuesArray': ['timestamp', 'type', 'value', 'score'],
            'respirationAveragesValuesArray': ['timestamp', 'average', 'high', 'low'],
            # Fallback : bodyBatteryValuesArray, heartRateValues, etc. sont gérés par le cas générique
        }
    
    # Cas 1 : Dict → récursion sur chaque clé
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            # Vérifier si cette clé est un cas spécial connu
            if key in known_mappings and isinstance(value, list) and value and isinstance(value[0], list):
                field_names = known_mappings[key]
                result[key] = [
                    dict(zip(field_names, item[:len(field_names)])) 
                    for item in value
                ]
                logging.debug(f"Transformed nested array at '{path}.{key}' using mapping: {field_names}")
            else:
                result[key] = flatten_nested_arrays(value, known_mappings, f"{path}.{key}")
        return result
    
    # Cas 2 : List → vérifier si c'est un nested array
    elif isinstance(obj, list):
        if not obj:
            return obj
        
        # Nested array détecté : [[...], [...]]
        if isinstance(obj[0], list):
            first_item_length = len(obj[0])
            
            # Cas 2a : Longueur 2 → fallback générique (timestamp, value)
            if first_item_length == 2:
                result = [{'timestamp': item[0], 'value': item[1]} for item in obj]
                logging.debug(f"Transformed generic 2-element nested array at '{path}'")
                return result
            
            # Cas 2b : Longueur > 2 → WARNING (devrait avoir un mapping explicite)
            else:
                logging.warning(
                    f"⚠️ Nested array with {first_item_length} elements found at '{path}' "
                    f"without explicit mapping. Consider adding to known_mappings. "
                    f"Using generic keys: val_0, val_1, ..."
                )
                result = [
                    {f'val_{i}': val for i, val in enumerate(item)}
                    for item in obj
                ]
                return result
        
        # Pas un nested array → récursion sur chaque élément
        else:
            return [flatten_nested_arrays(item, known_mappings, f"{path}[{i}]") for i, item in enumerate(obj)]
    
    # Cas 3 : Primitive (str, int, float, bool, None) → retour direct
    else:
        return obj
```

### ÉTAPE 2 : Modifier `fetcher.py` pour appliquer la transformation

**Import à ajouter** en haut de `src/connectors/garmin/fetcher.py` :

```python
from .utils import flatten_nested_arrays  # Ajouter après la ligne 11
```

**Modifications à faire** dans chaque méthode de fetch :

#### 2.1 Dans `_fetch_daily()` (lignes 83-98)

**Remplacer** :
```python
if data:
    # Normalize data structure
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                item["date"] = date_str
                item["data_type"] = metric_name
                results.append(item)
    elif isinstance(data, dict):
        data["date"] = date_str
        data["data_type"] = metric_name
        results.append(data)
    else:
        results.append({
            "date": date_str, 
            "data": data, 
            "data_type": = metric_name
        })
```

**Par** :
```python
if data:
    # Transform nested arrays first
    data = flatten_nested_arrays(data, path=f"{metric_name}.{date_str}")
    
    # Normalize data structure
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                item["date"] = date_str
                item["data_type"] = metric_name
                results.append(item)
    elif isinstance(data, dict):
        data["date"] = date_str
        data["data_type"] = metric_name
        results.append(data)
    else:
        results.append({
            "date": date_str, 
            "data": data, 
            "data_type": metric_name
        })
```

#### 2.2 Dans `_fetch_range()` (lignes 134-144)

**Remplacer** :
```python
if not data:
    return []
    
results = []
if isinstance(data, list):
    for item in data:
        if isinstance(item, dict):
            item["data_type"] = metric_name
        results.append(item)
elif isinstance(data, dict):
    data["data_type"] = metric_name
    results.append(data)
else:
    results.append({"data": data, "data_type": metric_name})
```

**Par** :
```python
if not data:
    return []

# Transform nested arrays
data = flatten_nested_arrays(data, path=metric_name)
    
results = []
if isinstance(data, list):
    for item in data:
        if isinstance(item, dict):
            item["data_type"] = metric_name
        results.append(item)
elif isinstance(data, dict):
    data["data_type"] = metric_name
    results.append(data)
else:
    results.append({"data": data, "data_type": metric_name})
```

#### 2.3 Dans `_fetch_simple()` (lignes 160-172)

**Remplacer** :
```python
if not data:
    return []
    
results = []
if isinstance(data, list):
    for item in data:
        if isinstance(item, dict):
            item["data_type"] = metric_name
        results.append(item)
else:
    # If it's a dict or primitive
    if isinstance(data, dict):
        data["data_type"] = metric_name
        results.append(data)
    else:
        results.append({"data": data, "data_type": metric_name})
```

**Par** :
```python
if not data:
    return []

# Transform nested arrays
data = flatten_nested_arrays(data, path=metric_name)
    
results = []
if isinstance(data, list):
    for item in data:
        if isinstance(item, dict):
            item["data_type"] = metric_name
        results.append(item)
else:
    # If it's a dict or primitive
    if isinstance(data, dict):
        data["data_type"] = metric_name
        results.append(data)
    else:
        results.append({"data": data, "data_type": metric_name})
```

#### 2.4 Dans `_fetch_activity_details()` (lignes 200-211)

**Remplacer** :
```python
try:
    details = client.get_activity_details(activity_id, maxchart=2000, maxpoly=4000)
    enriched = {
        **activity,
        "detailed_data": details,
        "data_type": "activity_details"
    }
    results.append(enriched)
    time.sleep(0.5)
except Exception as e:
    logging.warning(f"Failed details for {activity_id}: {e}")
```

**Par** :
```python
try:
    details = client.get_activity_details(activity_id, maxchart=2000, maxpoly=4000)
    
    # Transform nested arrays in activity and details
    clean_activity = flatten_nested_arrays(activity, path=f"activity_{activity_id}")
    clean_details = flatten_nested_arrays(details, path=f"details_{activity_id}")
    
    enriched = {
        **clean_activity,
        "detailed_data": clean_details,
        "data_type": "activity_details"
    }
    results.append(enriched)
    time.sleep(0.5)
except Exception as e:
    logging.warning(f"Failed details for {activity_id}: {e}")
```

#### 2.5 Dans `_fetch_activity_subdata()` (lignes 241-288)

**Pour le cas `activity_splits`** (lignes 242-262), remplacer :
```python
splits = client.get_activity_splits(activity_id)
typed_splits = client.get_activity_typed_splits(activity_id)
split_summaries = client.get_activity_split_summaries(activity_id)

data = {
    "activityId": activity_id,
    "activityName": activity.get("activityName", ""),
    "activityType": activity.get("activityType", ""),
    "startTimeLocal": activity.get("startTimeLocal", ""),
    "splits": splits,
    "typed_splits": typed_splits,
    "split_summaries": split_summaries,
    "data_type": metric_name
}
```

**Par** :
```python
splits = client.get_activity_splits(activity_id)
typed_splits = client.get_activity_typed_splits(activity_id)
split_summaries = client.get_activity_split_summaries(activity_id)

# Transform nested arrays
clean_splits = flatten_nested_arrays(splits, path=f"splits_{activity_id}")
clean_typed = flatten_nested_arrays(typed_splits, path=f"typed_splits_{activity_id}")
clean_summaries = flatten_nested_arrays(split_summaries, path=f"summaries_{activity_id}")

data = {
    "activityId": activity_id,
    "activityName": activity.get("activityName", ""),
    "activityType": activity.get("activityType", ""),
    "startTimeLocal": activity.get("startTimeLocal", ""),
    "splits": clean_splits,
    "typed_splits": clean_typed,
    "split_summaries": clean_summaries,
    "data_type": metric_name
}
```

**Pour les autres cas** (lignes 264-285), remplacer :
```python
# Standard subdata (weather, hr_zones, etc)
subdata = method(activity_id)
if subdata:
    data = {
        "activityId": activity_id,
        "activityName": activity.get("activityName", ""),
        "activityType": activity.get("activityType", ""),
        "startTimeLocal": activity.get("startTimeLocal", ""),
        f"{metric_name}_data": subdata,
        "data_type": metric_name
    }
```

**Par** :
```python
# Standard subdata (weather, hr_zones, etc)
subdata = method(activity_id)
if subdata:
    # Transform nested arrays
    clean_subdata = flatten_nested_arrays(subdata, path=f"{metric_name}_{activity_id}")
    
    data = {
        "activityId": activity_id,
        "activityName": activity.get("activityName", ""),
        "activityType": activity.get("activityType", ""),
        "startTimeLocal": activity.get("startTimeLocal", ""),
        f"{metric_name}_data": clean_subdata,
        "data_type": metric_name
    }
```

---

## ✅ Tests de Validation

### 1. Test unitaire de la fonction

Créer `test_flatten_nested_arrays.py` :

```python
from src.connectors.garmin.utils import flatten_nested_arrays

def test_simple_2_element():
    input_data = [[1000, 42], [2000, 43]]
    expected = [{'timestamp': 1000, 'value': 42}, {'timestamp': 2000, 'value': 43}]
    assert flatten_nested_arrays(input_data) == expected

def test_stress_values_4_element():
    input_data = {
        'stressValuesArray': [[1000, "MEASURED", 42, 3.0], [2000, "MEASURED", 43, 3.1]]
    }
    expected = {
        'stressValuesArray': [
            {'timestamp': 1000, 'type': 'MEASURED', 'value': 42, 'score': 3.0},
            {'timestamp': 2000, 'type': 'MEASURED', 'value': 43, 'score': 3.1}
        ]
    }
    assert flatten_nested_arrays(input_data) == expected

def test_nested_objects():
    input_data = {
        'activity': {
            'heartRate': [[1000, 120], [2000, 125]]
        }
    }
    expected = {
        'activity': {
            'heartRate': [
                {'timestamp': 1000, 'value': 120},
                {'timestamp': 2000, 'value': 125}
            ]
        }
    }
    assert flatten_nested_arrays(input_data) == expected
```

### 2. Test d'intégration (ingestion BigQuery)

```bash
# 1. Fetch fresh data avec transformation
python -m src.connectors.garmin --start-date 2025-11-22 --end-date 2025-11-22

# 2. Vérifier que les fichiers sont créés
ls -la *.jsonl

# 3. Tester l'ingestion
python test_ingestion.py

# 4. Vérifier qu'il n'y a AUCUNE erreur "Nested arrays not allowed"
```

### 3. Tests de régression

Comparer avec les anciens fichiers :
- Nombre de lignes identique
- Champs principaux présents
- Pas de perte de données (seulement transformation de structure)

---

## 🚨 Points d'Attention

### 1. Détection de nouveaux cas
Si Garmin ajoute un nouveau champ avec nested array >2 éléments :
- ✅ Le warning sera loggé
- ✅ La transformation générique `val_0, val_1` s'applique
- ⚠️ À terme, ajouter le mapping explicite dans `known_mappings`

### 2. Performance
La fonction est récursive → peut être lente sur de très gros objets.
- ✔️ OK pour Garmin (objets modérés, <1MB par activité)
- ⚠️ Si problème, envisager une version itérative

### 3. Compatibilité backward
Les fichiers existants (déjà ingérés) ne seront **pas** affectés.
- Les nouvelles ingestions auront le nouveau format
- Pour réingérer l'historique, refaire un fetch complet

---

## 📝 Checklist de Déploiement

- [ ] Ajouter `flatten_nested_arrays()` dans `utils.py`
- [ ] Importer la fonction dans `fetcher.py`
- [ ] Modifier `_fetch_daily()`
- [ ] Modifier `_fetch_range()`
- [ ] Modifier `_fetch_simple()`
- [ ] Modifier `_fetch_activity_details()`
- [ ] Modifier `_fetch_activity_subdata()` (2 cas)
- [ ] Créer les tests unitaires
- [ ] Tester sur un petit dataset (1 jour)
- [ ] Tester l'ingestion BigQuery
- [ ] Valider absence d'erreurs
- [ ] Déployer en production

---

## 🎓 Résumé

**Avant** :
```json
{"heartRate": [[1000, 120], [2000, 125]]}
```

**Après** :
```json
{"heartRate": [
  {"timestamp": 1000, "value": 120},
  {"timestamp": 2000, "value": 125}
]}
```

**Résultat** : ✅ Compatible BigQuery nativement, ✅ Autodétection parfaite, ✅ Maintenance = 0
