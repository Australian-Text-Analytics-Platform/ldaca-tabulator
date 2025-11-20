
from src.utils import (load_config,
                       load_table_from_db,
                       drop_id_columns)
import pandas as pd
import json
from rocrate_tabular.tabulator import ROCrateTabulator
from pathlib import Path
from unittest.mock import patch, MagicMock
from pathlib import Path
from io import BytesIO
from tempfile import mkdtemp

def test_load_config(path = "./tests/crates/minimal/ro-crate-metadata.json"):
    config_file = load_config(path)
    assert '@graph' in config_file


def _create_db(tb: ROCrateTabulator, folder: str | Path | None = None, db_name: str = "crate.db"):
    folder_path = Path(folder or mkdtemp())
    folder_path.mkdir(parents=True, exist_ok=True)

    database_path = folder_path / db_name
    database_path.parent.mkdir(parents=True, exist_ok=True)

    tb.crate_to_db(str(folder_path), str(database_path))
    return folder_path, database_path

def test_load_table_from_db():
    tb = ROCrateTabulator()
    _, database_path = _create_db(tb = tb , folder="tests/crates/languageFamily", db_name="languageFamily.db")
    tb.infer_config()
    tb.use_tables(["RepositoryObject"])
    df = load_table_from_db(database_path, "RepositoryObject")
    assert not df.empty



def _df_from_json_ids(json_path="./config/config-ids.json"):
    """
    Data frame only includes columns not rows. As we are interested in removing columns of ids
    """
    # Load JSON config
    with open(json_path, "r") as f:
        config = json.load(f)

    id_cols = config.get("ids", [])

    data = {
        "name": [],    # non-ID property 1
        "text": [],  # non-ID property 2
    }

    return pd.DataFrame(data)


def test_drop_id_columns():
    df = _df_from_json_ids()
    assert set(drop_id_columns(df).columns) == {"name", "text"}

