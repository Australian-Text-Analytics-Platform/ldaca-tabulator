# ========== Python Standard Library ==========
import importlib.resources
import json
import re
import shutil
import sqlite3
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
from urllib.parse import unquote, urlparse

# ========== Third-Party Dependencies ==========
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ========== Project-Specific Imports ==========
from rocrate_tabular.tabulator import ROCrateTabulator

# -------------------------
# Constants
# -------------------------
GENERAL_CONFIG = "./configs/general/general-config.json"
CORPUS_CONFIG_DIR = "./configs/corpora"
TEXT_PROP = "ldac:mainText"


# -------------------------------------------------------------
# Class responsible for loading, unpacking, and processing
# LDaCA RO-Crate corpora into clean, analysis-ready tables.
# -------------------------------------------------------------
@dataclass
class LDaCATabulator:
    """
    Loader and processor for LDaCA RO-Crate corpora.

    This class wraps around ``ROCrateTabulator`` package to provide a simple interface
    for extracting a corpus, accessing its tables, and applying configuration-
    based cleaning rules. It aims to make LDaCA corpora easy to inspect and
    convert into clean, analysis-ready DataFrames with minimal user code.

    Parameters
    ----------
    url : str
        URL of the zipped RO-Crate corpus.

    Attributes
    ----------
    url : str
        The original corpus URL.
    tb : ROCrateTabulator
        The underlying tabulator instance used to inspect the crate.
    database : path-like
        Path to the extracted SQLite database.
    extract_to : path-like
        Directory where the corpus archive was extracted.
    """

    url: str
    text_prop: str = TEXT_PROP
    tb: ROCrateTabulator = field(default_factory=ROCrateTabulator)

    def __post_init__(self):
        self.database, self.extract_to = self._unzip_corpus(self.url, tb=self.tb)

        self.tb.config = self.load_config(GENERAL_CONFIG)

        self.tb.text_prop = self.text_prop

    # -----------------------------------------------
    # Helper methods
    # -----------------------------------------------

    @staticmethod
    def _make_clean_name(value: str) -> str:
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
        return safe_name or "rocrate"

    @staticmethod
    def _get_corpus_name_from_metadata(extract_to: Path, zip_url: str) -> str | None:
        """Read the corpus display name from ro-crate-metadata.json, if available."""
        metadata_path = extract_to / "ro-crate-metadata.json"
        if not metadata_path.exists():
            return None

        data = json.loads(metadata_path.read_text(encoding="utf-8"))
        graph = data.get("@graph", [])

        parsed = urlparse(zip_url)
        corpus_id = unquote(Path(parsed.path).name).removesuffix(".zip")

        corpus_node = next(
            (item for item in graph if item.get("@id") == corpus_id), None
        )
        if corpus_node and corpus_node.get("name"):
            return str(corpus_node["name"])

        dataset_node = next(
            (
                item
                for item in graph
                if (
                    ("Dataset" in item.get("@type", []))
                    if isinstance(item.get("@type"), list)
                    else item.get("@type") == "Dataset"
                )
                and item.get("name")
            ),
            None,
        )
        if dataset_node:
            return str(dataset_node["name"])

        return None

    @staticmethod
    def _unique_storage_names(
        extract_root: Path,
        db_root: Path,
        base_name: str,
    ) -> tuple[str, str]:
        """Return non-conflicting folder/db names based on base_name."""
        safe_base = LDaCATabulator._make_clean_name(base_name)
        candidate = safe_base
        idx = 2
        while (extract_root / candidate).exists() or (
            db_root / f"{candidate}.db"
        ).exists():
            candidate = f"{safe_base}_{idx}"
            idx += 1
        return candidate, f"{candidate}.db"

    @staticmethod
    def _names_from_zip_url(zip_url: str) -> tuple[str, str]:
        """Build initial storage names from the decoded corpus filename."""
        parsed = urlparse(zip_url)
        base_name = unquote(Path(parsed.path).name).removesuffix(".zip") or "rocrate"
        safe_name = LDaCATabulator._make_clean_name(base_name)
        folder_name = safe_name
        db_name = f"{safe_name}.db"
        return folder_name, db_name

    @staticmethod
    def _metadata_matches_zip_url(metadata_path: Path, zip_url: str) -> bool:
        """Return True when metadata appears to describe the corpus from zip_url."""
        if not metadata_path.exists():
            return False

        try:
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
        except OSError, json.JSONDecodeError:
            return False

        graph = data.get("@graph", [])
        parsed = urlparse(zip_url)
        corpus_id = unquote(Path(parsed.path).name).removesuffix(".zip")
        if not corpus_id:
            return False

        candidates = {corpus_id, f"./{corpus_id}"}
        return any(
            item.get("@id") in candidates for item in graph if isinstance(item, dict)
        )

    @staticmethod
    def _find_existing_extract_for_url(extract_root: Path, zip_url: str) -> Path | None:
        """Find an already-extracted corpus folder matching zip_url."""
        if not extract_root.exists():
            return None

        for child in extract_root.iterdir():
            if not child.is_dir():
                continue
            metadata_path = child / "ro-crate-metadata.json"
            if LDaCATabulator._metadata_matches_zip_url(metadata_path, zip_url):
                return child

        return None

    @staticmethod
    def _get_corpus_node_from_preview(extract_to: Path, zip_url: str) -> dict | None:
        """Read the corpus metadata node from ro-crate-preview.html, if available."""
        html_path = extract_to / "ro-crate-preview.html"
        if not html_path.exists():
            return None

        html_content = html_path.read_text(encoding="utf-8")
        soup = BeautifulSoup(html_content, "html.parser")
        script_tag = soup.find("script", type="application/ld+json")
        if script_tag is None or script_tag.string is None:
            return None

        json_data = json.loads(script_tag.string)
        parsed_url = urlparse(zip_url)
        encoded_name = Path(parsed_url.path).name.removesuffix(".zip")
        corpus_id = unquote(encoded_name)

        corpus_node = next(
            (
                item
                for item in json_data.get("@graph", [])
                if item.get("@id") == corpus_id
            ),
            None,
        )
        return corpus_node if isinstance(corpus_node, dict) else None

    def _unzip_corpus(
        self,
        zip_url: str,
        tb: ROCrateTabulator,
        folder_name: str | None = None,
        db_name: str | None = None,
        overwrite: bool = True,
    ):
        """
        Download, extract, and tabulate an RO-Crate corpus into a database.

        This function downloads a ZIP archive from a given URL of LDaCA corpus, extracts its
        contents into a local folder, and converts the extracted RO-Crate dataset
        into a database.
        """

        user_provided_folder = folder_name is not None
        user_provided_db = db_name is not None

        default_folder_name, default_db_name = self._names_from_zip_url(zip_url)
        if folder_name is None:
            folder_name = default_folder_name
        if db_name is None:
            db_name = default_db_name

        cwd = Path.cwd()
        extract_root = cwd / "ldacaCollections"
        db_root = cwd / "databases"
        extract_root.mkdir(parents=True, exist_ok=True)
        db_root.mkdir(parents=True, exist_ok=True)

        extract_to = extract_root / folder_name
        database = db_root / db_name

        if not user_provided_folder and not user_provided_db:
            cached_extract = self._find_existing_extract_for_url(extract_root, zip_url)
            if cached_extract is not None:
                extract_to = cached_extract
                folder_name = cached_extract.name
                database = db_root / f"{cached_extract.name}.db"
                overwrite = True
            elif not overwrite and extract_to.exists():
                overwrite = True

        zip_file = extract_root / f"{folder_name}.zip"

        if overwrite:
            database.unlink(missing_ok=True)

        metadata_path = extract_to / "ro-crate-metadata.json"
        needs_download = overwrite or (not metadata_path.exists())

        if extract_to.exists() and overwrite:
            shutil.rmtree(extract_to)

        if needs_download:
            extract_to.mkdir(parents=True, exist_ok=True)
            with requests.get(zip_url, stream=True, timeout=20) as resp:
                resp.raise_for_status()
                with open(zip_file, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            with zipfile.ZipFile(zip_file, "r") as zf:
                zf.extractall(extract_to)

            zip_file.unlink(missing_ok=True)

            if not user_provided_folder and not user_provided_db:
                corpus_name = self._get_corpus_name_from_metadata(extract_to, zip_url)
                if corpus_name:
                    desired_folder = self._make_clean_name(corpus_name)
                    if desired_folder and desired_folder != extract_to.name:
                        desired_extract_to = extract_root / desired_folder
                        desired_db = f"{desired_folder}.db"
                        if desired_extract_to.exists():
                            if overwrite:
                                shutil.rmtree(desired_extract_to)
                            else:
                                desired_folder, desired_db = self._unique_storage_names(
                                    extract_root,
                                    db_root,
                                    desired_folder,
                                )
                                desired_extract_to = extract_root / desired_folder

                        shutil.move(str(extract_to), str(desired_extract_to))
                        extract_to = desired_extract_to
                        database = db_root / desired_db

        tb.crate_to_db(str(extract_to), str(database))
        return database, extract_to

    @staticmethod
    def _load_package_config(path_parts: List[str]):
        """Load configuration from the package resources."""
        return LDaCATabulator.load_config(str(Path(*path_parts)))

    @staticmethod
    def load_config(config_path: str):
        """Load and parse a JSON configuration file."""
        path_obj = Path(config_path)
        candidate_paths = [path_obj]
        if not path_obj.is_absolute():
            candidate_paths.append(Path(__file__).resolve().parents[2] / path_obj)

        for candidate in candidate_paths:
            if candidate.exists():
                with candidate.open(encoding="utf-8") as f:
                    return json.load(f)

        resource_path = importlib.resources.files("ldacatabulator")
        for part in path_obj.parts:
            resource_path = resource_path.joinpath(part)

        with resource_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def drop_id_columns(df):
        """Remove identifier-like columns from a pandas DataFrame."""
        cols_to_drop = [c for c in df.columns if "_id" in c]
        return df.drop(columns=cols_to_drop, errors="ignore")

    def _load_entity_table(self, table_name: str, columns: List[str] | None = None):
        """Load an entity table from the extracted SQLite database."""
        try:
            self.tb.entity_table(table_name)
        except Exception:
            print("No %s table in this corpus.", table_name)
            return None

        with sqlite3.connect(self.database) as conn:
            if columns:
                cols = ", ".join(f'"{c}"' for c in columns)
            else:
                cols = "*"

            query = f"SELECT {cols} FROM {table_name}"
            df = pd.read_sql(query, conn)

        return self.drop_id_columns(df)

    @staticmethod
    def drop_high_null_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns whose missing-value proportion exceeds 0.99."""
        max_null_prop = 0.99
        if not 0 <= max_null_prop <= 1:
            raise ValueError("max_null_prop must be between 0 and 1")

        null_prop = df.isna().mean()
        keep_mask = null_prop <= max_null_prop
        return df.loc[:, keep_mask]

    def get_text(self, full_df: bool = False):
        """Load the RepositoryObject table and return it in cleaned form."""
        df = self._load_entity_table("RepositoryObject")
        if not full_df:
            df = self.drop_high_null_columns(df)
        return self.drop_id_columns(df)

    def get_people(self, full_df: bool = False):
        """Load and return the Person table from the corpus in cleaned form."""
        df = self._load_entity_table("Person")
        if not full_df:
            df = self.drop_high_null_columns(df)
        return df

    def get_organization(self, full_df: bool = False):
        """Load and return the Organization table from the corpus in cleaned form."""
        df = self._load_entity_table("Organization")
        if not full_df:
            df = self.drop_high_null_columns(df)
        return df

    def get_speaker(self, full_df: bool = False):
        """Load and return the Speaker table from the corpus in cleaned form."""
        df = self._load_entity_table("Speaker")
        if not full_df:
            df = self.drop_high_null_columns(df)
        return df

    def get_name(self) -> str:
        """Return the corpus display name from extracted RO-Crate metadata."""
        corpus_name = self._get_corpus_name_from_metadata(self.extract_to, self.url)
        if corpus_name:
            return corpus_name

        corpus_node = self._get_corpus_node_from_preview(self.extract_to, self.url)
        if corpus_node and corpus_node.get("name"):
            return str(corpus_node["name"])

        if not corpus_name:
            raise ValueError(
                "Could not determine corpus name from ro-crate-metadata.json or ro-crate-preview.html."
            )

        return corpus_name

    def get_corpus_info(self):
        """Extract and return corpus metadata from the extracted RO-Crate preview HTML."""
        corpus_node = self._get_corpus_node_from_preview(self.extract_to, self.url)
        if corpus_node is None:
            parsed_url = urlparse(self.url)
            encoded_name = Path(parsed_url.path).name.removesuffix(".zip")
            corpus_id = unquote(encoded_name)
            raise ValueError(f"Could not find corpus metadata node for '{corpus_id}'.")

        corpus_name = corpus_node.get("name")
        corpus_description = corpus_node.get("description")
        date_published = corpus_node.get("datePublished")

        html_path = self.extract_to / "ro-crate-preview.html"
        html_content = html_path.read_text(encoding="utf-8")
        soup = BeautifulSoup(html_content, "html.parser")
        script_tag = soup.find("script", type="application/ld+json")
        if script_tag is None or script_tag.string is None:
            raise ValueError(
                "Could not find JSON-LD metadata in ro-crate-preview.html."
            )
        json_data = json.loads(script_tag.string)

        publisher_field = corpus_node.get("publisher")
        publisher_id = None
        if isinstance(publisher_field, list) and publisher_field:
            first = publisher_field[0]
            publisher_id = first.get("@id") if isinstance(first, dict) else first
        elif isinstance(publisher_field, dict):
            publisher_id = publisher_field.get("@id")
        elif isinstance(publisher_field, str):
            publisher_id = publisher_field

        publisher_node = next(
            (
                item
                for item in json_data.get("@graph", [])
                if item.get("@id") == publisher_id
            ),
            None,
        )
        publisher = (
            publisher_node.get("name")
            if isinstance(publisher_node, dict)
            else "Unknown"
        )

        return (
            f"## Name: \n{corpus_name}\n\n"
            f"## Description: \n{corpus_description}\n\n"
            f"## Date Published\n{date_published}\n\n"
            f"## Publisher\n{publisher}"
        )

    def corpus_specific_tables_list(self) -> str:
        """Return a list of corpus-specific tables defined in this corpus' config."""
        match = re.search(r"~(\d+)\.", self.url)
        if not match:
            return "Could not extract corpus ID from URL. Cannot load config."
        corpus_id = match.group(1)

        config = self.load_config(f"{CORPUS_CONFIG_DIR}/{corpus_id}.json")
        tables = list(config.get("tables", {}).keys())

        return (
            f"Corpus-specific tables: {tables}. "
            f"Use corpus_specific_tables(table_name) to load the data."
        )

    def corpus_specific_tables(self, table: str):
        """Load and return a cleaned corpus-specific table."""
        match_obj = re.search(r"~(\d+)\.", self.url)
        if not match_obj:
            raise ValueError(
                "Could not extract corpus ID from URL. Cannot load config."
            )
        match = match_obj.group(1)

        self.tb.config = self.load_config(f"{CORPUS_CONFIG_DIR}/{match}.json")

        return self._load_entity_table(table)
