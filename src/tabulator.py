from rocrate_tabular.tabulator import ROCrateTabulator
from .utils import (unzip_corpus,
                    load_config,
                    load_table_from_db,
                    drop_id_columns)
import sqlite3
import pandas as pd

class LDaCATabulator:

    def __init__(self, url, config_path="./configs/general/general-config.json", text_prop="ldac:mainText"):
        self.url = url
        self.tb = ROCrateTabulator()

        # Download and unzip
        self.database, self.extract_to = unzip_corpus(url, tb=self.tb)

        # Load LDaCA config 
        self.tb.config = load_config(config_path)

        # What property contains the text file
        self.tb.text_prop = text_prop

    def _filter_ignored_columns(self, table_name, df):
        
        config = self.tb.config
        tables_cfg = config.get("tables", {})
        table_cfg = tables_cfg.get(table_name, {})

        cols = list(df.columns)
        to_drop = set()

        # Direct ignore_props for this table
        direct_ignores = table_cfg.get("ignore_props", [])
        for prop in direct_ignores:
            for c in cols:
                if c == prop or c.startswith(f"{prop}_"):
                    to_drop.add(c)

        # Expanded properties
        expand_props = table_cfg.get("expand_props", [])
        all_ignored_props = set()
        for t_cfg in tables_cfg.values():
            for ip in t_cfg.get("ignore_props", []):
                all_ignored_props.add(ip)

        for exp in expand_props:  
            for ip in all_ignored_props:
                prefix = f"{exp}_{ip}"
                for c in cols:
                    if c == prefix or c.startswith(prefix + "_"):
                        to_drop.add(c)

        if to_drop:
            df = df.drop(columns=[c for c in to_drop if c in df.columns])

        return df
    
    def _load_entity_table(self, table_name: str):
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
        self.tb.entity_table("RepositoryObject")

        # Load main RepositoryObject table
        df = load_table_from_db(str(self.database), "RepositoryObject")

        # ignore_props 
        df = self._filter_ignored_columns("RepositoryObject", df)

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
        return self._load_entity_table("Person")
 

    # ------------------------------------------------------------
    # get_organization()
    # ------------------------------------------------------------
    def get_organization(self):
        return self._load_entity_table("Organization")
    

    # ------------------------------------------------------------
    # get_speaker()
    # ------------------------------------------------------------
    # TODO remove id variables
    def get_speaker(self):
        df = self._load_entity_table("Speaker")
        return drop_id_columns(df)

