"""
Garmin Connector Utilities
--------------------------
Helper functions for date handling, file I/O, and logging.
"""
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from zoneinfo import ZoneInfo

from .config import DEFAULT_TIMEZONE

def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    fmt = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True # Ensure we override any existing config
    )

def to_jsonl(data: List[Dict[str, Any]], jsonl_output_path: str) -> None:
    """Write list of dicts to JSONL file."""
    with open(jsonl_output_path, 'w', encoding='utf-8') as f:
        for entry in data:
            f.write(json.dumps(entry, default=str) + '\n')

def write_jsonl(data: List[Dict[str, Any]], output_path: Path) -> None:
    """Write a list of dicts to a JSONL file with directory creation."""
    try:
        if not data:
            logging.warning(f"No data to write for {output_path}")
            return

        output_path.parent.mkdir(parents=True, exist_ok=True)
        to_jsonl(data, jsonl_output_path=str(output_path))
        logging.info(f"📁 Dump saved to: {output_path} ({len(data)} items)")
    except Exception as e:
        raise IOError(f"Failed to write JSONL file: {e}") from e

def generate_output_filename(
    output_dir: Path, data_type: str, timezone: str = DEFAULT_TIMEZONE
) -> Path:
    """Generate timestamped output filename."""
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        logging.warning(f"Invalid timezone {timezone}, falling back to UTC")
        from datetime import timezone as dt_timezone
        tz = dt_timezone.utc
        
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    return output_dir / f"{timestamp}_garmin_{data_type}.jsonl"

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
            'floorValuesArray': ['start_time', 'end_time', 'ascended', 'descended'],
            'spO2SingleValues': ['timestamp', 'value', 'type'],
            'bodyBatteryValuesArray': {
                2: ['timestamp', 'value'],
                4: ['timestamp', 'type', 'value', 'score']
            }
        }
    
    # Cas 1 : Dict → récursion sur chaque clé
    if isinstance(obj, dict):
        # Special handling for Garmin activity details metrics
        # Transform [val1, val2, ...] into {"key1": val1, "key2": val2} using descriptors
        # This avoids NULL values in BigQuery arrays and provides a meaningful schema
        if 'metricDescriptors' in obj and 'activityDetailMetrics' in obj:
            try:
                descriptors = obj['metricDescriptors']
                metrics_list = obj['activityDetailMetrics']
                
                # Create mapping: index -> key
                index_map = {d['metricsIndex']: d['key'] for d in descriptors if 'metricsIndex' in d and 'key' in d}
                
                new_metrics = []
                for item in metrics_list:
                    if not isinstance(item, dict) or 'metrics' not in item:
                        continue
                    
                    raw_values = item['metrics']
                    if not isinstance(raw_values, list):
                        continue

                    structured_metric = {}
                    for i, value in enumerate(raw_values):
                        # Skip None values to avoid BigQuery errors and sparse data
                        if value is not None and i in index_map:
                            structured_metric[index_map[i]] = value
                    
                    new_metrics.append(structured_metric)
                
                obj['activityDetailMetrics'] = new_metrics
                logging.debug(f"Transformed activityDetailMetrics at '{path}' using descriptors")
            except Exception as e:
                logging.warning(f"Failed to transform activityDetailMetrics at '{path}': {e}")

        result = {}
        for key, value in obj.items():
            # Skip None values to avoid BigQuery "Only optional fields can be set to NULL" error
            # for fields that are detected as REQUIRED (e.g. metrics in activity_details)
            if value is None:
                continue
            
            # Replace empty dicts with None - BigQuery auto-detection can't handle empty structs
            if isinstance(value, dict) and not value:
                result[key] = None
                continue

            # Vérifier si cette clé est un cas spécial connu
            if key in known_mappings and isinstance(value, list) and value and isinstance(value[0], list):
                mapping = known_mappings[key]
                field_names = None
                
                # Determine mapping based on length
                item_len = len(value[0])
                if isinstance(mapping, dict):
                    field_names = mapping.get(item_len)
                elif isinstance(mapping, list):
                    # Legacy list support: use if length matches or just try to zip?
                    # Safer to check length if possible, but for backward compat we just use it.
                    # Ideally we should check if len(mapping) == item_len
                    field_names = mapping

                if field_names:
                    result[key] = [
                        dict(zip(field_names, item[:len(field_names)])) 
                        for item in value
                    ]
                    logging.debug(f"Transformed nested array at '{path}.{key}' using mapping: {field_names}")
                else:
                    # Fallback to recursion if no mapping found for this length
                    # This allows the generic fallback in Cas 2 to handle it (e.g. logging warning)
                     result[key] = flatten_nested_arrays(value, known_mappings, f"{path}.{key}")
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
