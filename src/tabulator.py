from rocrate_tabular.tabulator import ROCrateTabulator
from src.utils import unzip_corpus
from collections import defaultdict
import json
import pandas as pd 
import sqlite3

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

    def build_table(self, verbose: bool = True):  # this could be function for Alex app
        # prepare tables
        self.tb.infer_config()
        target_types = list(self.tb.config["potential_tables"])

        # TODO bug in rocrate-tabulator 
        if "Language" in target_types:
            target_types.remove("Language")

        table = "RepositoryObject"
        self.tb.use_tables(target_types)

        config = self.tb.config["tables"][table]
        if not config.get("all_props"):
            self.tb.entity_table(table)

        ids = self.tb.fetch_ids(table)

        prop_types = defaultdict(set)

        # Scan each entity’s properties
        for entity_id in ids:
            for prop in self.tb.fetch_properties(entity_id):
                tgt = prop.get("target_id")
                if not tgt:
                    continue
                # Get all @type values for the target
                types = {
                    p["value"]
                    for p in self.tb.fetch_properties(tgt)
                    if p["property_label"] == "@type"
                }
                if types:
                    prop_types[prop["property_label"]].update(types)

        # Filter to properties whose target types intersect the desired list
        candidates = [
            prop
            for prop, types in prop_types.items()
            if types & set(target_types)
            and prop in config.get("all_props", [])
        ]

        if not candidates:
            if verbose:
                print("No expandable properties matched those target types.")
            return prop_types

        if verbose:
            print(f"Expanding {len(candidates)} properties:", candidates)

        # Expand and rebuild table
        self.tb.expand_properties(table, candidates)
        # Adding text to the table
        self.tb.entity_table(table, "ldac:indexableText")

        # Read data from DB
        with sqlite3.connect(self.database) as conn:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)

        with open("./config/config.json") as f:
            config = json.load(f)

        result = [(key, v) for key, val in prop_types.items() for v in val]
        combined_props = []

        for prop_name, target_type in result:
            # Only proceed if target_type exists in config["tables"]
            if target_type in config["tables"]:
                subprops = config["tables"][target_type]["properties"]
                for subprop in subprops:
                    combined_props.append(f"{prop_name}_{subprop}")

        # TODO: Why are some columns in df and combined_props not matching?
        # Get only those RepositoryObject properties that exist in df
        repo_props = [
            p for p in config['tables']['RepositoryObject']['properties']
            if p in df.columns
        ]

        valid_combined_props = list(set(combined_props) & set(df.columns))

        # Concatenate
        clean_data = pd.concat(
            [df[repo_props], df[valid_combined_props]],
            axis=1
        )

        return clean_data


    ############################### specific data user may need can use these functions
    def get_text():
        pass

    def get_csv():
        pass
    

    # xml files are not that common
    def get_xml(): 
        pass

    def get_people():
        pass