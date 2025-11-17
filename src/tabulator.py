from rocrate_tabular.tabulator import ROCrateTabulator
from .utils import (unzip_corpus,
                    load_config,
                    load_table_from_db,
                    drop_id_columns)

class LDaCATabulator:

    def __init__(self, url, config_path="./config/cooee-config.json", text_prop="ldac:mainText"):
        self.url = url
        self.tb = ROCrateTabulator()

        # Download and unzip
        self.database, self.extract_to = unzip_corpus(url, tb=self.tb)

        # Load LDaCA config 
        self.tb.config = load_config(config_path)

        # What property contains the text file
        self.tb.text_prop = text_prop

    # ------------------------------------------------------------
    # get_text()
    # ------------------------------------------------------------
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

    # get_text: RepositoryObject
    def get_text(self):
        #TODO some properties mat not be expandable for some corpora
        self.tb.entity_table("RepositoryObject")
        df = load_table_from_db(str(self.database), "RepositoryObject")
        df = self._filter_ignored_columns("RepositoryObject", df)
        df = drop_id_columns(df) 
        return df

    # ------------------------------------------------------------
    # get_people()
    # ------------------------------------------------------------

    # TODO it does not remove properties of ids
    def get_people(self):

        try:
            self.tb.entity_table("Person")
        except Exception:
            print("No Person table in this corpus.")
            return None

        df = load_table_from_db(str(self.database), "Person")
        return drop_id_columns(df) 

    # ------------------------------------------------------------
    # get_organization()
    # ------------------------------------------------------------
    def get_organization(self):

        try:
            self.tb.entity_table("Organization")
        except Exception:
            print("No Organization table in this corpus.")
            return None

        df = load_table_from_db(str(self.database), "Organization")
        return drop_id_columns(df) 
    
    def get_speaker(self):
 
        try:
            self.tb.entity_table("Speaker")
        except Exception:
            print("No Speaker table in this corpus.")
            return None

        df = load_table_from_db(str(self.database), "Speaker")
        return drop_id_columns(df) 

