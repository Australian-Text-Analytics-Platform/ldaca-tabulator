from rocrate_tabular.tabulator import ROCrateTabulator
from .utils import (unzip_corpus,
                    load_config,
                    load_table_from_db,
                    drop_id_columns)
import sqlite3
import pandas as pd
import re
from dataclasses import dataclass, field
from pathlib import Path

GENERAL_CONFIG = "./configs/general/general-config.json"
CORPUS_CONFIG_DIR = "./configs/corpora/"
TEXT_PROP = "ldac:mainText"

@dataclass
class LDaCATabulator:
    """
    Loader and processor for LDaCA RO-Crate corpora.

    This class wraps around ``ROCrateTabulator`` to provide a simple interface
    for extracting a corpus, accessing its tables, and applying configuration-
    based cleaning rules. It aims to make LDaCA corpora easy to inspect and
    convert into clean, analysis-ready DataFrames with minimal user code.

    Parameters
    ----------
    url : str
        URL of the zipped RO-Crate corpus (http://).

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
    config_path: str = GENERAL_CONFIG
    tb: ROCrateTabulator = field(default_factory=ROCrateTabulator)
    database: Path | None = None
    extract_to: Path | None = None

    # Download and unzip, load config, set test property
    def __post_init__(self):
        self.database, self.extract_to = unzip_corpus(
        self.url,
        tb=self.tb
        )
        
        self.tb.config = load_config(self.config_path)
        
        self.tb.text_prop = self.text_prop
    
    def _load_entity_table(self, table_name: str):
        """
        Load an entity table from the extracted SQLite database.

        This method checks whether the table exists in the RO-Crate, loads it
        from the database, and applies standard cleaning by removing internal
        identifier columns.

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
            print(f"No {table_name} table in this corpus.")
            return None
        
        df = load_table_from_db(str(self.database), table_name)
        return df

    # ------------------------------------------------------------
    # get_text()
    # ------------------------------------------------------------
    # TODO This method may need to be made more memory efficient. 
    def get_text(self):
        """
        Load the RepositoryObject table and return it in a cleaned form.
        If speaker information is available in the corpus, a separate column
        containing a list of speaker names is added to each record.

        Returns
        -------
        pandas.DataFrame
        The cleaned RepositoryObject table, with speakers included when a
        corresponding junction table is present.
        """

        self.tb.entity_table("RepositoryObject")

        # Load main RepositoryObject table
        df = load_table_from_db(str(self.database), "RepositoryObject")
        
        # The speaker junction table 
        speaker_junction = "RepositoryObject_ldac:speaker"
        
        conn = sqlite3.connect(str(self.database))

        # Check if table exists in the DB
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table';",
            conn
        )["name"].tolist()
        
        if speaker_junction not in tables:
            return drop_id_columns(df)
        else:

            # Load junction table
            junction = pd.read_sql_query(
                f"SELECT * FROM '{speaker_junction}'", conn
            )

            # Load Person table
            people = self._load_entity_table("Person")

            # Merge to attach person names to each observation
            merged = junction.merge(
                people,
                left_on="entity_id",
                right_on="entity_id",
                how="left"
            )

            # Group speakers by RepositoryObject_id
            speaker_names = (
                merged.groupby("entity_id")["name"]
                    .apply(lambda x: list(x.dropna()))
                    .reset_index()
                    .rename(columns={"name": "speakers"})
            )

            # Attach speakers list to RepositoryObject table
            df = df.merge(
                speaker_names,
                left_on="entity_id",
                right_on="entity_id",
                how="left"
            ).drop(columns=["entity_id"], errors="ignore")

            # Fix any NaN speakers to empty lists
            df["speakers"] = df["speakers"].apply(
                lambda x: x if isinstance(x, list) else []
            )

        return drop_id_columns(df)


    # ------------------------------------------------------------
    # get_people()
    # ------------------------------------------------------------
    
    def get_people(self):
        """
        Load and return the Person table from the corpus in a cleaned form.

        Returns
        -------
        pandas.DataFrame or None
        The cleaned Person table, or ``None`` if the corpus does not contain
        a Person entity.
        """
        
        df = self._load_entity_table("Person")

        return drop_id_columns(df)
 

    # ------------------------------------------------------------
    # get_organization()
    # ------------------------------------------------------------
    def get_organization(self):
        """
        Load and return the Organization table from the corpus in a cleaned form.

        Returns
        -------
        pandas.DataFrame or None
        The cleaned Organization table, or ``None`` if the corpus does not
        contain an Organization entity.
        """
        
        df = self._load_entity_table("Organization")
        
        return drop_id_columns(df)
    

    # ------------------------------------------------------------
    # get_speaker()
    # ------------------------------------------------------------
    
    def get_speaker(self):
        """
        Load and return the Speaker table from the corpus in a cleaned form.

        Returns
        -------
        pandas.DataFrame or None
        The cleaned Speaker table, or ``None`` if the corpus does not contain
        a Speaker entity.
        """
        
        df  = self._load_entity_table("Speaker")
        return drop_id_columns(df)
    
    # -------------------------------------------------------------
    # corpus_specific_tables
    # -------------------------------------------------------------
    def corpus_specific_tables_list(self) -> str:
        """
        Return a list of corpus-specific tables defined in this corpus' config file.

        This method extracts the numeric corpus identifier from the corpus URL,
        loads the corresponding configuration file, and returns the names of the 
        tables that are specific to that corpus. These tables are defined under 
        the ``"tables"`` section of the config JSON.

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
        config = load_config(f"{CORPUS_CONFIG_DIR}{corpus_id}.json")

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
    
        self.tb.config = load_config(f"{CORPUS_CONFIG_DIR}{match}.json")
        
        df = self._load_entity_table(table)
        return drop_id_columns(df)
        

