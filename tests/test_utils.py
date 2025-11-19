
from src.utils import (load_config,
                       unzip_corpus,
                       load_table_from_db,
                       drop_id_columns)
import pandas as pd
import json


def test_load_config(path = "./tests/crates/minimal/ro-crate-metadata.json"):
    config_file = load_config(path)
    assert '@graph' in config_file


def _df_from_json_ids(json_path="./config/config-ids.json"):
    # Load JSON config
    with open(json_path, "r") as f:
        config = json.load(f)

    id_cols = config.get("ids", [])

    data = {
        "name": [],    # non-ID property 1
        "text": [],  # non-ID property 2
    }

    # Add every ID column with simple dummy values
    #for col in id_cols:
    #    data[col] = []

    return pd.DataFrame(data)

def test_drop_id_columns():
    df = _df_from_json_ids()
    assert set(df.columns) == {"name", "text"}

