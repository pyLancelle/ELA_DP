import os
import json
import logging
from dotenv import load_dotenv
import pandas as pd
import yaml


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
