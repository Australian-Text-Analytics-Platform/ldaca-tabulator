from rocrate_tabular.tabulator import ROCrateTabulator
from .utils import (unzip_corpus,
                    load_config,
                    load_table_from_db,
                    drop_id_columns)
import sqlite3
import pandas as pd

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
        URL of the zipped RO-Crate corpus (arcp:// or http://).
    config_path : str, optional
        Path to the LDaCA configuration JSON controlling per-table rules.
    text_prop : str, optional
        Property name containing the main text content for RepositoryObjects.

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


    def __init__(self, url, config_path="./configs/general/general-config.json", text_prop="ldac:mainText"):
        self.url = url
        self.tb = ROCrateTabulator()

        # Download and unzip
        self.database, self.extract_to = unzip_corpus(url, tb=self.tb)

        # Load LDaCA config 
        self.tb.config = load_config(config_path)

        # What property contains the text file
        self.tb.text_prop = text_prop
    
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
        return drop_id_columns(df)

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

        # drop id columns
        df = drop_id_columns(df)
        
        # The speaker junction table 
        speaker_junction = "RepositoryObject_ldac:speaker"
        
        conn = sqlite3.connect(str(self.database))

        # Check if table exists in the DB
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table';",
            conn
        )["name"].tolist()
        
        if speaker_junction not in tables:
            pass
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

            # Group speakers by RepositoryObject_id → ONLY names
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

        return df


    # ------------------------------------------------------------
    # get_people()
    # ------------------------------------------------------------

    # TODO it does not remove properties of ids
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
        return self._load_entity_table("Organization")
    

    # ------------------------------------------------------------
    # get_speaker()
    # ------------------------------------------------------------
    # TODO remove id variables
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
        

