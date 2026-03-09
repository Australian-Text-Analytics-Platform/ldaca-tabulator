from io import BytesIO
from pathlib import Path
import sqlite3
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
import pytest

from src.ldacatabulator.tabulator import LDaCATabulator


# --------------------------------------------------------------------
# Helper to bypass __post_init__ (which extracts ZIP)
# --------------------------------------------------------------------
def _blank_instance():
    """
    Create an LDaCATabulator instance without running __post_init__
    to avoid running get.request() function.
    
    """
    return LDaCATabulator.__new__(LDaCATabulator)


def _make_zip_bytes() -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("ro-crate-metadata.json", '{"@context": "https://w3id.org/ro/crate/1.1/context"}')
    return buf.getvalue()


# --------------------------------------------------------------------
# Test: _unzip_corpus
# --------------------------------------------------------------------
def test_unzip_corpus(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    zip_bytes = _make_zip_bytes()

    mock_response = MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_content.return_value = [zip_bytes]

    fake_tb = MagicMock()

    tab = _blank_instance()

    with patch("src.ldacatabulator.tabulator.requests.get", return_value=mock_response):
        db_path, extracted_path = LDaCATabulator._unzip_corpus(
            tab,
            zip_url="http://fake-url.com/zip",
            tb=fake_tb,
            folder_name="testCorpus",
            db_name="testCorpus.db",
            overwrite=True,
        )

    # Assertions
    assert extracted_path.exists()
    assert len(list(extracted_path.iterdir())) > 0
    assert db_path == Path.cwd() / "testCorpus.db"
    fake_tb.crate_to_db.assert_called_once_with(
        str(Path.cwd() / "testCorpus"),
        str(Path.cwd() / "testCorpus.db"),
    )


# --------------------------------------------------------------------
# Test: load_config
# --------------------------------------------------------------------
def test_load_config():
    config = LDaCATabulator.load_config(
        "tests/crates/minimal/ro-crate-metadata.json"
    )
    assert "@graph" in config


# --------------------------------------------------------------------
# Test: _load_entity_table
# --------------------------------------------------------------------
def test_load_entity_table(tmp_path):
    db_path = tmp_path / "test.db"

    # Create fake DB table
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE RepositoryObject (id TEXT, value TEXT)")
    conn.execute("INSERT INTO RepositoryObject VALUES ('1', 'abc')")
    conn.commit()
    conn.close()

    inst = _blank_instance()
    inst.database = db_path
    inst.tb = MagicMock()

    df = LDaCATabulator._load_entity_table(inst, "RepositoryObject")

    assert not df.empty
    assert list(df.columns) == ["id", "value"]
    inst.tb.entity_table.assert_called_once_with("RepositoryObject")


# --------------------------------------------------------------------
# Helper df for testing drop_id_columns
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
    

def test_get_text():
    tab = _blank_instance()
    raw = pd.DataFrame(
        {
            "text": ["a"],
            "kept": [1],
            "name_id": ["x"],
            "mostly_null": [None],
        }
    )

    with patch.object(tab, "_load_entity_table", return_value=raw) as mock_load:
        df = tab.get_text()

    assert isinstance(df, pd.DataFrame)
    assert "name_id" not in df.columns
    assert "mostly_null" not in df.columns
    mock_load.assert_called_once_with("RepositoryObject")


def test_get_people():
    tab = _blank_instance()
    raw = pd.DataFrame({"name": ["person"], "mostly_null": [None]})

    with patch.object(tab, "_load_entity_table", return_value=raw) as mock_load:
        df = tab.get_people()

    assert isinstance(df, pd.DataFrame)
    assert "mostly_null" not in df.columns
    mock_load.assert_called_once_with("Person")


def test_get_organization():
    tab = _blank_instance()
    raw = pd.DataFrame({"name": ["org"], "mostly_null": [None]})

    with patch.object(tab, "_load_entity_table", return_value=raw) as mock_load:
        df = tab.get_organization()

    assert isinstance(df, pd.DataFrame)
    assert "mostly_null" not in df.columns
    mock_load.assert_called_once_with("Organization")

