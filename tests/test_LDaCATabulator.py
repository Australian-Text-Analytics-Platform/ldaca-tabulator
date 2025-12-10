
from src.tabulator import (#load_config,
                           #load_table_from_db,
                           #drop_id_columns,
                           #unzip_corpus,
                           LDaCATabulator)
import pandas as pd
from rocrate_tabular.tabulator import ROCrateTabulator
from pathlib import Path
from unittest.mock import patch, MagicMock
from tempfile import mkdtemp
import shutil
import zipfile
import sqlite3

import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from tempfile import mkdtemp

from rocrate_tabular.tabulator import ROCrateTabulator
from src.tabulator import LDaCATabulator


# --------------------------------------------------------------------
# Helper to bypass __post_init__ (which downloads/extracts ZIP)
# --------------------------------------------------------------------
def _blank_instance():
    """Create an LDaCATabulator instance without running __post_init__."""
    return LDaCATabulator.__new__(LDaCATabulator)


# --------------------------------------------------------------------
# Test: _unzip_corpus
# --------------------------------------------------------------------
def test_unzip_corpus(tmp_path):
    zip_path = Path("tests/crates/languageFamily.zip")
    zip_bytes = zip_path.read_bytes()

    mock_response = MagicMock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = MagicMock()

    fake_tb = MagicMock()

    tab = _blank_instance()

    with patch("requests.get", return_value=mock_response):
        db_path, extracted_path = LDaCATabulator._unzip_corpus(
            tab,
            zip_url="http://fake-url.com/zip",
            tb=fake_tb,
            folder_name="testCorpus",
            db_name="testCorpus.db",
        )

    # Assertions
    assert extracted_path.exists()
    assert len(list(extracted_path.iterdir())) > 0
    assert db_path == Path.cwd() / "testCorpus.db"
    fake_tb.crate_to_db.assert_called_once()


# --------------------------------------------------------------------
# Test: load_config (static method)
# --------------------------------------------------------------------
def test_load_config():
    config = LDaCATabulator.load_config(
        "tests/crates/minimal/ro-crate-metadata.json"
    )
    assert "@graph" in config


# --------------------------------------------------------------------
# Helper: create a minimal DB from a folder
# --------------------------------------------------------------------
def _create_db(tb: ROCrateTabulator, folder: str | Path, db_name: str):
    folder_path = Path(folder)
    database_path = folder_path / db_name
    tb.crate_to_db(str(folder_path), str(database_path))
    return database_path


# --------------------------------------------------------------------
# Test: _load_table_from_db
# --------------------------------------------------------------------
def test_load_table_from_db(tmp_path):
    db_path = tmp_path / "test.db"

    # Create fake DB table
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE RepositoryObject (id TEXT, value TEXT)")
    conn.execute("INSERT INTO RepositoryObject VALUES ('1', 'abc')")
    conn.commit()
    conn.close()

    inst = _blank_instance

    df = LDaCATabulator._load_table_from_db(
        inst,
        str(db_path),
        "RepositoryObject"
    )

    assert not df.empty
    assert list(df.columns) == ["id", "value"]


# --------------------------------------------------------------------
# Helper DF for testing drop_id_columns
# --------------------------------------------------------------------
def _df_from_json_ids():
    return pd.DataFrame({
        "name": [],
        "text": [],
        "name_id": [],
        "name_id_1": []
    })


# --------------------------------------------------------------------
# Test: drop_id_columns (static method)
# --------------------------------------------------------------------
def test_drop_id_columns():
    df = _df_from_json_ids()
    out = LDaCATabulator.drop_id_columns(df)
    assert set(out.columns) == {"name", "text"}

def mock_requests_get(*args, **kwargs):
    # Load local zip file into memory
    with open("tests/crates/languageFamily.zip", "rb") as f:
        content = f.read()

    class MockResponse:
        def __init__(self, content):
            self.content = content
        def raise_for_status(self):
            pass

    return MockResponse(content)


@patch("requests.get", side_effect=mock_requests_get)
def test_ldaca_tabulator_load_local_zip(_mock_get):
    # URL is irrelevant now; it is intercepted
    fake_url = "https://example.com/not-real.zip"
    tab = LDaCATabulator(fake_url)

    # Test a method that requires the ZIP
    df = tab.get_text()

    # Validate the data was processed
    assert df is not None
    assert not df.empty

    

