# ========== Python Standard Library ==========
import json
import re
import shutil
import sqlite3
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import logging

# ========== Third-Party Dependencies ==========
import pandas as pd
import requests

# ========== Project-Specific Imports ==========
from rocrate_tabular.tabulator import ROCrateTabulator

# -------------------------
# Constants
# -------------------------
GENERAL_CONFIG = "./configs/general/general-config.json"
CORPUS_CONFIG_DIR = "./configs/corpora/"
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
        
        # Download and unzip
        self.database, self.extract_to = self._unzip_corpus(
        self.url,
        tb=self.tb
        )
        
        self.tb.config = self.load_config(GENERAL_CONFIG)
        
        self.tb.text_prop = self.text_prop
        
    # -----------------------------------------------
    # Helper methods
    # -----------------------------------------------
    
    def _unzip_corpus(
        self,
        zip_url: str,
        tb: ROCrateTabulator,
        folder_name: str | None = None,
        db_name: str | None = None,
        overwrite: bool = False, #HACK If already exist, it may give error or use the same corpus. Use default True
        ):
        """
        Download, extract, and tabulate an RO-Crate corpus into a database.

        This function downloads a ZIP archive from a given URL of LDaCA corpus, extracts its
        contents into a local folder, and converts the extracted RO-Crate dataset
        into a database.

        Parameters
        ----------
        zip_url : str
            URL pointing to the ZIP file containing the RO-Crate corpus.
        tb : ROCrateTabulator
            Instance of ROCrateTabulator used to convert the extracted crate into
            a database via `crate_to_db()`.
        folder_name : str | None, optional
            Name of the directory to extract the corpus into. Defaults to
            `"rocrate"` if not provided.
        db_name : str | None, optional
            Name of the output SQLite database file. Defaults to
            `"{folder_name}.db"` if not provided.
        overwrite : bool, optional
            If `True` and the target extraction folder already exists, it will be
            deleted and recreated before extraction. If `False` and the folder
            already exists, no download or extraction occurs and the existing
            folder is used. Default is `False`.

        Returns
        -------
        tuple[pathlib.Path, pathlib.Path]
            A tuple `(database_path, extract_path)` referring to:
            - `database_path`: Path to the generated SQLite DB.
            - `extract_path` : Path where the ZIP was extracted.

        Notes
        -----
        - If `overwrite=False` and the folder already exists, the ZIP file is
        not downloaded or re-extracted; the existing content is used.
        - `crate_to_db()` is always called, meaning the database will be built or
        updated regardless of extraction behavior.
        """

        # Resolve target names
        if folder_name is None:
            folder_name = "rocrate"
        if db_name is None:
            db_name = f"{folder_name}.db"

        cwd = Path.cwd()
        extract_to = cwd / folder_name
        database = cwd / db_name
        
        # To save downloaded zip
        zip_file = cwd / f"{folder_name}.zip"

        # Prepare destination
        if extract_to.exists():
            if overwrite:
                shutil.rmtree(extract_to)
                extract_to.mkdir(parents=True, exist_ok=True)
        else:
            extract_to.mkdir(parents=True, exist_ok=True)

            # Download and extract
            with requests.get(zip_url, stream=True, timeout=20) as resp:
                resp.raise_for_status()
                with open(zip_file, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1e6):
                        if chunk:
                            f.write(chunk)

            with zipfile.ZipFile(zip_file, "r") as zf:
                zf.extractall(extract_to)

            zip_file.unlink()

        # Build (or connect) DB
        tb.crate_to_db(str(extract_to), str(database))
        return database, extract_to
    
    
    # loading config file
    @staticmethod
    def load_config(config_path: str):
        """
        Load and parse a JSON configuration file.

        Parameters
        ----------
        config_path : str
            Path to the JSON configuration file.

        Returns
        -------
        dict
            Parsed configuration data.
        """
        with open(config_path) as f:
            config = json.load(f)
        return config
    
    # loading table from database
    def _load_table_from_db(
        self,
        database_path: str,
        table_name: str,
        columns: List[str] | None = None
        ):
        """
        Load a table from a SQLite database into a pandas DataFrame.

        Parameters
        ----------
        database_path : str
            Path to the SQLite database file.
        table_name : str
            Name of the table to load from the database.
        columns : list[str] | None, optional
            List of column names to select. If None (default), all columns
            in the table will be selected.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing the selected table data.
        """
 
        with sqlite3.connect(database_path) as conn:
            if columns:
                cols = ", ".join(f'"{c}"' for c in columns)
            else:
                cols = "*"

            query = f"SELECT {cols} FROM {table_name}"
            return pd.read_sql(query, conn)
    
    @staticmethod
    def drop_id_columns(df):
        """
        Remove identifier-like columns from a pandas DataFrame.

        This function drops any column whose name contains the substring "_id".
        It is a general-purpose utility for removing ID or foreign-key columns 
        that are typically not useful for end-user analysis. 
    
        Examples of columns removed:
            - "author_id"
            - "conformsTo_id"
            - "conformsTo_1_id"
            - "author_id_1"
            - "ldac:speaker_id"

        The match is substring-based, so any column name containing "_id" 
        anywhere will be removed. Use with caution if your dataset includes 
        non-identifier fields that also contain "_id" in their names.

        Parameters
        ----------
        df : pandas.DataFrame
            Input DataFrame from which ID-related columns will be removed.

        Returns
        -------
        pandas.DataFrame
            A new DataFrame with all "_id" columns dropped. Columns that do not 
            exist are ignored safely.
        """
        cols_to_drop = [c for c in df.columns if "_id" in c]
        return df.drop(columns=cols_to_drop, errors="ignore")

    
    def _load_entity_table(
        self,
        table_name: str):
        """
        Load an entity table from the extracted SQLite database.

        This method checks whether the table exists in the RO-Crate, loads it
        from the database.

        Parameters
        ----------
        table_name : str
            Name of the entity table to load.

        Returns
        -------
        pandas.DataFrame or None
            The loaded and cleaned table, or ``None`` if the table is not
            present in the corpus.
        """
        try:
            self.tb.entity_table(table_name)
        except Exception:
            logging.info("No %s table in this corpus.", table_name)
            return None
        
        df = self._load_table_from_db(str(self.database), table_name)
        
        return self.drop_id_columns(df)
    
    # ------------------------------------------------------------
    # Class methods
    # ------------------------------------------------------------

    
    # get_text() method
    def get_text(self):
        """
        Load the RepositoryObject table (a table that contain text) and return it in a cleaned form.

        Returns
        -------
        pandas.DataFrame
        The cleaned RepositoryObject table.
        """
        
        self.tb.entity_table("RepositoryObject")

        df = self._load_table_from_db(str(self.database), "RepositoryObject")

        return self.drop_id_columns(df)

    # get_people() method
    def get_people(self):
        """
        Load and return the Person table from the corpus in a cleaned form.

        Returns
        -------
        pandas.DataFrame or None
        The cleaned Person table, or ``None`` if the corpus does not contain
        a Person entity.
        """

        return self._load_entity_table("Person")
    
    # get_organization() method
    def get_organization(self):
        """
        Load and return the Organization table from the corpus in a cleaned form.

        Returns
        -------
        pandas.DataFrame or None
        The cleaned Organization table, or ``None`` if the corpus does not
        contain an Organization entity.
        """
        
        return self._load_entity_table("Organization")
    
    # get_speaker() method
    def get_speaker(self):
        """
        Load and return the Speaker table from the corpus in a cleaned form.

        Returns
        -------
        pandas.DataFrame or None
        The cleaned Speaker table, or ``None`` if the corpus does not contain
        a Speaker entity.
        """
        
        return self._load_entity_table("Speaker")
    
    # -------------------------------------------------------------
    # corpus_specific_tables
    # -------------------------------------------------------------
    def corpus_specific_tables_list(self) -> str:
        """
        Return a list of corpus-specific tables defined in this corpus' config file.

        This method extracts the numeric corpus identifier from the corpus URL,
        loads the corresponding configuration file, and returns the names of the 
        tables that are specific to that corpus.

        Returns
        -------
        str
            A user-friendly message listing the available tables and guiding the
            user to call ``corpus_specific_tables(table_name)`` to load the data.
        """
        # Extract corpus ID from the URL (digits after "~" and before ".")
        match = re.search(r'~(\d+)\.', self.url)
        if not match:
            return "Could not extract corpus ID from URL. Cannot load config."
        corpus_id = match.group(1)

        # Load the specific corpus config file
        # Adjust this path depending on how your configs are stored
        config = self.load_config(f"{CORPUS_CONFIG_DIR}{corpus_id}.json")

        # Extract table names from the loaded config
        tables = list(config.get("tables", {}).keys())

        return (
            f"Corpus-specific tables: {tables}. "
            f"Use corpus_specific_tables(table_name) to load the data."
        )

      
    def corpus_specific_tables(self, table: str):
        """
        Load and return a cleaned corpus-specific table.

        This method:
        1. Extracts the numeric corpus identifier from the corpus URL.
        2. Loads the corresponding per-corpus configuration file located at
           ``configs/corpora/``.
        3. Updates ``self.tb.config`` with that configuration.
        4. Loads the requested table using ``_load_entity_table``.
        5. Removes ID-like columns using ``drop_id_columns``.

        Parameters
        ----------
        table : str
            Name of the corpus-specific table to load.

        Returns
        -------
        pandas.DataFrame
            The cleaned DataFrame for the requested table.
        """
        
        match = re.search(r'~(\d+)\.', self.url).group(1)
    
        self.tb.config = self.load_config(f"{CORPUS_CONFIG_DIR}{match}.json")
        
        return self._load_entity_table(table)
        

