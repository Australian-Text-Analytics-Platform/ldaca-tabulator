from src.tabulator import LDaCATabulator
import pandas as pd
from rocrate_tabular.tabulator import ROCrateTabulator
from pathlib import Path
from unittest.mock import patch, MagicMock
from tempfile import mkdtemp
import sqlite3
import pytest


# --------------------------------------------------------------------
# Helper to bypass __post_init__ (which downloads/extracts ZIP)
# --------------------------------------------------------------------
def _blank_instance():
    """
    Create an LDaCATabulator instance without running __post_init__
    to avoid running get.request() function.
    """
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
# Test: load_config
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
# Test: drop_id_columns
# --------------------------------------------------------------------
def test_drop_id_columns():
    df = _df_from_json_ids()
    out = LDaCATabulator.drop_id_columns(df)
    assert set(out.columns) == {"name", "text"}
    

# --------------------------------------------------------------------
# Test: methods in class, that is, get_text(), get_people(), 
#       and get_organization
# --------------------------------------------------------------------    
@pytest.fixture
def tabulator():
    """
    Fixture that:
    - Loads your local test ZIP
    - Mocks requests.get()
    - Creates a fully initialized LDaCATabulator
    """

    zip_path = Path("tests/crates/languageFamily.zip")
    zip_bytes = zip_path.read_bytes()

    # Create fake HTTP response
    mock_response = MagicMock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = MagicMock()

    # Mock requests.get globally for this fixture
    with patch("requests.get", return_value=mock_response):
        tab = LDaCATabulator("https://example.com/fake.zip")

    return tab

def test_get_text(tabulator):
    df = tabulator.get_text()
    assert df is not None
    assert not df.empty

    
def test_get_people(tabulator):
    df = tabulator.get_people()
    assert df is not None
    assert isinstance(df, pd.DataFrame)


def test_get_organization(tabulator):
    df = tabulator.get_organization()
    assert df is not None
    assert isinstance(df, pd.DataFrame)


