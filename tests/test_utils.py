
from src.utils import (load_config,
                       load_table_from_db,
                       drop_id_columns,
                       unzip_corpus)
import pandas as pd
from rocrate_tabular.tabulator import ROCrateTabulator
from pathlib import Path
from unittest.mock import patch, MagicMock
from pathlib import Path
from tempfile import mkdtemp
from pathlib import Path
from unittest.mock import patch, MagicMock

def test_unzip_corpus(tmp_path):
    # Path to your real test ZIP file
    zip_path = Path("tests/crates/languageFamily.zip")

    # Read actual zip bytes
    zip_bytes = zip_path.read_bytes()

    # Mock response from requests.get
    mock_response = MagicMock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = MagicMock()

    # Mock ROCrateTabulator
    fake_tb = MagicMock()

    # Use patch to replace requests.get with our fake one
    with patch("requests.get", return_value=mock_response):
        db_path, extracted_path = unzip_corpus(
            zip_url="http://fake-url.com/zip",
            tb=fake_tb,
            folder_name="testCorpus",
            db_name="testCorpus.db",
        )

    # ---- Assertions ----
    # Folder created
    assert extracted_path.exists()

    # Check at least 1 file extracted (your ZIP has files)
    extracted_files = list(extracted_path.iterdir())
    assert len(extracted_files) > 0

    # DB path correct
    assert db_path == Path.cwd() / "testCorpus.db"

    # Ensure crate_to_db was called
    fake_tb.crate_to_db.assert_called_once()

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



def _df_from_json_ids():
    """
    Data frame only includes columns not rows. 
    As we are interested in removing columns of ids
    """

    data = {
        "name": [],  
        "text": [],
        "name_id": [],
        "name_id_1": []
    }

    return pd.DataFrame(data)


def test_drop_id_columns():
    df = _df_from_json_ids()
    assert set(drop_id_columns(df).columns) == {"name", "text"}


