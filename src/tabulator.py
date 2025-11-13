from rocrate_tabular.tabulator import ROCrateTabulator
from .utils import (
    unzip_corpus,
    load_config,
    load_table_from_db,
    auto_ignore_bad_props
    )
import pandas as pd
from collections import defaultdict

# main idea
'''
from ldaca.tabulator import LDaCATabulator

CORPUS=""https://data.ldaca.edu.au/api/object/meta?resolve-parts&noUrid&id=arcp%3A%2F%2Fname%2Chdl10.25949~24769173.v1"

tb = new LDaCATabulator(url=CORPUS)

tb.build_tables()

df = tb.get_texts()
print(df.head)

                              entity_id identifier  ...                    author_prov:specializationOf_id author_description
0  arcp://name,hdl10.26180~23961609/item/1-001      1-001  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
1  arcp://name,hdl10.26180~23961609/item/1-002      1-002  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
2  arcp://name,hdl10.26180~23961609/item/1-003      1-003  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
3  arcp://name,hdl10.26180~23961609/item/1-004      1-004  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
4  arcp://name,hdl10.26180~23961609/item/1-005      1-005  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None

[5 rows x 53 columns]


df2 = tb.get_people()
print(df2.head)

                                           entity_id                                      name  ... homeLocation_id description
0  https://www.peterlang.com/search?f_0=author&q_...                       Clemens W. A. Fritz  ...            None        None
1  arcp://name,hdl10.26180~23961609/author/Philip...  Philip, Arthur - status 1788 text #1-001  ...            None        None
2  arcp://name,hdl10.26180~23961609/author/Philip...                            Philip, Arthur  ...            None        None
3   arcp://name,hdl10.26180~23961609/recipient/1-001                           1-001 Recipient  ...     #place_GB-E        None
4  arcp://name,hdl10.26180~23961609/author/Philip...  Philip, Arthur - status 1788 text #1-002  ...            None        None

[5 rows x 22 columns]


'''

class LDaCATabulator:
    def __init__(self, url):
        self.url = url
        self.tb = ROCrateTabulator()
        self.database, self.extract_to = unzip_corpus(self.url, tb=self.tb)

    def build_table(self, verbose: bool = True):
        """
        Clean version of build_table() following the official Tabulator usage.
        - Load config
        - Build crate DB
        - Build all tables declared in config
        - Return RepositoryObject as a DataFrame
        """

        # 1️⃣ Load config
        if verbose:
            print("Loading LDaCA config...")
        self.tb.config = load_config("./config/cooee-config.json")

        # 2️⃣ Build full DB from crate
        if verbose:
            print("Building crate into SQLite database...")
        self.tb.crate_to_db(self.extract_to, self.database)

        # 3️⃣ Build tables listed in config
        tables = self.tb.config["tables"].keys()

        for table in tables:
            try:
                if verbose:
                    print(f"Building table '{table}'...")
                self.tb.entity_table(table)
            except Exception as e:
                # Some tables won’t exist in some corpora — skip safely
                if verbose:
                    print(f"⚠️ Skipping table '{table}': {e}")

        # 4️⃣ Load RepositoryObject DataFrame
        if verbose:
            print("Loading RepositoryObject table...")

        df = load_table_from_db(self.database, "RepositoryObject")

        return df

    def get_text(self, include_metadata: bool = True): # This method can also be used for Alex app
        table = "RepositoryObject"
        if include_metadata:
            return self.build_table()

        self.tb.infer_config()
        self.tb.use_tables([table])
        self.tb.entity_table(table, "ldac:indexableText")

        # Read data from DB
        df = load_table_from_db(self.database, table, columns=["ldac:mainText", "ldac:indexableText"])

        return df
            
    
    # Michael comment on issue #78: first of LDaCA tabulator should return it as text 
    def get_csv():
        pass
    

    # PT comment on issue #78: we do not deal with XML yet
    def get_xml(): 
        pass

    def get_people(self):
        table = "Person"

        if table not in self.tb.infer_config():
            print(f"No '{table}' table found in the corpus.")
            return None

        self.tb.infer_config()
        self.tb.use_tables([table])

        # Read data from DB
        df = load_table_from_db(self.database, table)
        
        config = load_config("./config/config.json")
        
        repo_props = [
            p for p in config['tables']['Person'].get('properties', [])
            if p in df.columns
            ]

        df = df[repo_props]
        return df

    def get_organization(self):
        table = "Organization"

        if table not in self.tb.infer_config():
            print(f"No '{table}' table found in the corpus.")
            return None
        
        self.tb.infer_config()
        self.tb.use_tables([table])

        # Read data from DB
        df = load_table_from_db(self.database, table)
        
        config = load_config("./config/config.json")
        
        repo_props = [
            p for p in config['tables']['Organization'].get('properties', [])
            if p in df.columns
            ]

        df = df[repo_props]
        return df
