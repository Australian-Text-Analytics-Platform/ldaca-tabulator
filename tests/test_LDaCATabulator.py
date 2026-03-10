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


def _make_zip_bytes_with_metadata(metadata: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("ro-crate-metadata.json", metadata)
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


def test_unzip_corpus_uses_metadata_corpus_name_for_default_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    metadata = """
    {
      "@graph": [
        {
          "@id": "fake-corpus",
          "@type": "Dataset",
          "name": "Fancy Corpus Name"
        }
      ]
    }
    """
    zip_bytes = _make_zip_bytes_with_metadata(metadata)

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
            zip_url="http://fake-url.com/fake-corpus.zip",
            tb=fake_tb,
        )

    assert extracted_path.name == "Fancy_Corpus_Name"
    assert db_path.name == "Fancy_Corpus_Name.db"
    fake_tb.crate_to_db.assert_called_once_with(
        str(Path.cwd() / "Fancy_Corpus_Name"),
        str(Path.cwd() / "Fancy_Corpus_Name.db"),
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


def test_load_entity_table_missing_table_returns_none(tmp_path):
    inst = _blank_instance()
    inst.database = tmp_path / "test.db"
    inst.tb = MagicMock()
    inst.tb.entity_table.side_effect = Exception("missing")

    df = LDaCATabulator._load_entity_table(inst, "MissingTable")

    assert df is None


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


def test_drop_high_null_columns():
    df = pd.DataFrame(
        {
            "keep_edge_99pct": [None] * 99 + [1],
            "drop_100pct": [None] * 100,
            "keep_full": list(range(100)),
        }
    )
    out = LDaCATabulator.drop_high_null_columns(df)
    assert set(out.columns) == {"keep_edge_99pct", "keep_full"}
    

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


def test_get_speaker():
    tab = _blank_instance()
    raw = pd.DataFrame({"name": ["speaker"], "mostly_null": [None]})

    with patch.object(tab, "_load_entity_table", return_value=raw) as mock_load:
        df = tab.get_speaker()

    assert isinstance(df, pd.DataFrame)
    assert "mostly_null" not in df.columns
    mock_load.assert_called_once_with("Speaker")


def test_post_init_sets_config_and_text_prop():
    fake_tb = MagicMock()
    expected_config = {"tables": {}}

    with patch.object(
        LDaCATabulator,
        "_unzip_corpus",
        return_value=(Path("/tmp/rocrate.db"), Path("/tmp/rocrate")),
    ) as mock_unzip, patch.object(
        LDaCATabulator,
        "load_config",
        return_value=expected_config,
    ) as mock_load_config:
        tab = LDaCATabulator("https://example.com/fake.zip", text_prop="ldac:testText", tb=fake_tb)

    assert tab.database == Path("/tmp/rocrate.db")
    assert tab.extract_to == Path("/tmp/rocrate")
    assert fake_tb.config == expected_config
    assert fake_tb.text_prop == "ldac:testText"
    mock_unzip.assert_called_once()
    mock_load_config.assert_called_once()


def test_corpus_specific_tables_list():
    tab = _blank_instance()
    tab.url = "https://example.com/~23089559.zip"

    with patch.object(LDaCATabulator, "load_config", return_value={"tables": {"TableA": {}, "TableB": {}}}) as mock_cfg:
        result = tab.corpus_specific_tables_list()

    assert "TableA" in result
    assert "TableB" in result
    mock_cfg.assert_called_once_with("./configs/corpora/23089559.json")


def test_corpus_specific_tables_list_invalid_url():
    tab = _blank_instance()
    tab.url = "https://example.com/no-corpus-id.zip"

    result = tab.corpus_specific_tables_list()

    assert "Could not extract corpus ID" in result


def test_corpus_specific_tables_updates_config_and_loads():
    tab = _blank_instance()
    tab.url = "https://example.com/~24769173.zip"
    tab.tb = MagicMock()
    expected_df = pd.DataFrame({"x": [1]})
    cfg = {"tables": {"MyTable": {}}}

    with patch.object(LDaCATabulator, "load_config", return_value=cfg) as mock_cfg, patch.object(
        tab, "_load_entity_table", return_value=expected_df
    ) as mock_load_table:
        result = tab.corpus_specific_tables("MyTable")

    assert result.equals(expected_df)
    assert tab.tb.config == cfg
    mock_cfg.assert_called_once_with("./configs/corpora/24769173.json")
    mock_load_table.assert_called_once_with("MyTable")


def test_get_corpus_info(tmp_path):
    html = """
    <html>
      <body>
        <script type="application/ld+json">
        {
          "@graph": [
            {
              "@id": "test corpus",
              "name": "Test Corpus",
              "description": "Corpus description",
              "datePublished": "2025-01-01",
              "publisher": [{"@id": "pub-1"}]
            },
            {
              "@id": "pub-1",
              "name": "LDaCA Publisher"
            }
          ]
        }
        </script>
      </body>
    </html>
    """
    html_path = tmp_path / "ro-crate-preview.html"
    html_path.write_text(html, encoding="utf-8")

    tab = _blank_instance()
    tab.url = "https://example.com/download/test%20corpus.zip"
    tab.extract_to = tmp_path

    out = tab.get_corpus_info()

    assert "Test Corpus" in out
    assert "Corpus description" in out
    assert "2025-01-01" in out
    assert "LDaCA Publisher" in out


def test_default_storage_names_are_collection_specific():
    name1 = LDaCATabulator._default_storage_names("https://example.com/download/~123.zip")
    name2 = LDaCATabulator._default_storage_names("https://example.com/download/~456.zip")

    assert name1 != name2
    assert name1[0] == "123"
    assert name1[1].endswith(".db")
