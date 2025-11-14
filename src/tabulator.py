from rocrate_tabular.tabulator import ROCrateTabulator
from .utils import unzip_corpus, load_config, load_table_from_db

class LDaCATabulator:
    """
    A minimal wrapper with exactly 3 methods:
        - get_text()
        - get_people()
        - get_organization()
    ignore_props and expand_props are handled directly by ROCrateTabulator.
    """

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
        """
        Drop columns from df based on ignore_props in the config.
        Works for any corpus / config.
        """
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
        self.tb.entity_table("RepositoryObject")
        df = load_table_from_db(str(self.database), "RepositoryObject")
        df = self._filter_ignored_columns("RepositoryObject", df)
        return df

    # ------------------------------------------------------------
    # get_people()
    # ------------------------------------------------------------

    # TODO it does not remove properties of ids
    def get_people(self):
        """
        Return Person table (metadata).
        """
        try:
            self.tb.entity_table("Person")
        except Exception:
            print("No Person table in this corpus.")
            return None

        return load_table_from_db(str(self.database), "Person")

    # ------------------------------------------------------------
    # get_organization()
    # ------------------------------------------------------------
    def get_organization(self):
        """
        Return Organization metadata.
        """
        try:
            self.tb.entity_table("Organization")
        except Exception:
            print("No Organization table in this corpus.")
            return None

        return load_table_from_db(str(self.database), "Organization")
    
    def get_speaker(self):
        """
        Return Speaker metadata.
        """
        try:
            self.tb.entity_table("Speaker")
        except Exception:
            print("No Speaker table in this corpus.")
            return None

        return load_table_from_db(str(self.database), "Speaker")

