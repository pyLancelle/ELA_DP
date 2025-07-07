import os
import json
import logging
from dotenv import load_dotenv
import pandas as pd
import yaml
from typing import Union, List


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_token(env_var: str) -> str:
    load_dotenv()
    token = os.getenv(env_var)
    if not token:
        raise RuntimeError(f"Vous devez définir la variable d'environnement {env_var}")
    return token


def get_settings():
    settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
    with open(settings_path) as f:
        return yaml.safe_load(f)


def dump_nested_csv(df: pd.DataFrame, filename: str) -> None:
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(
                lambda x: (
                    json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list))
                    else x
                )
            )
    df.to_csv(filename, index=False)


def to_jsonl(
    data: Union[List[dict], dict], jsonl_output_path: str, key: str = "items"
) -> None:
    """
    Convertit une liste d'objets (ou un dict contenant une clé liste) en fichier JSONL.
    Ne stocke PAS le JSON d'origine, n'exploite que les objets en mémoire.

    Args:
        data (list | dict): Liste d'objets, ou dict contenant la clé (par ex. 'items')
        jsonl_output_path (str): Chemin du fichier de sortie JSONL
        key (str): Clé à chercher si data est un dict (défaut 'items')
    """
    if isinstance(data, dict):
        items = data.get(key, [])
    else:
        items = data

    with open(jsonl_output_path, "w", encoding="utf-8") as fout:
        for item in items:
            json.dump(item, fout, ensure_ascii=False)
            fout.write("\n")
