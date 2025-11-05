
import zipfile
import requests 
from io import BytesIO
from rocrate_tabular.tabulator import ROCrateTabulator
from collections import defaultdict
import itertools
from pathlib import Path
from typing import Tuple
import shutil
import json
import pandas as pd 
import sqlite3


def unzip_corpus(
    zip_url: str,
    tb: ROCrateTabulator,
    folder_name: str | None = None,
    db_name: str | None = None,
    overwrite: bool = False,
):

    # Resolve target names/paths
    if folder_name is None:
        folder_name = Path(zip_url).stem or "rocrate"
    if db_name is None:
        db_name = f"{folder_name}.db"

    cwd = Path.cwd()
    extract_to = cwd / folder_name
    database = cwd / db_name

    # Prepare destination
    if extract_to.exists():
        if overwrite:
            shutil.rmtree(extract_to)
            extract_to.mkdir(parents=True, exist_ok=True)
    else:
        extract_to.mkdir(parents=True, exist_ok=True)

        # Download and extract
        resp = requests.get(zip_url, stream=True)
        resp.raise_for_status()
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            zf.extractall(extract_to)

    # Build (or connect) DB
    tb.crate_to_db(str(extract_to), str(database))
    return database, extract_to


def expand_for_entity_types(
    zip_url: str,
    table: str,
    folder_name: str,
    db_name: str,
    sample: int | None = None,
    verbose: bool = True,
):

    tb = ROCrateTabulator()

    database, extract_to = unzip_corpus(zip_url, tb, folder_name= folder_name, db_name= db_name)

    # prepare tables
    tb.infer_config() # Not sure if I need this here. Need to check
    target_types = list(tb.config["potential_tables"])

    if "Language" in target_types:
        target_types.remove("Language") # maybe we need to remove Repository Object form the list to make sure we are not expanding RepoObject again

    tb.use_tables(target_types)

    config = tb.config["tables"][table]
    if not config.get("all_props"):
        tb.entity_table(table)

    ids = tb.fetch_ids(table)
    if sample:
        ids = itertools.islice(ids, sample)

    prop_types = defaultdict(set)

    # Scan each entity’s properties
    for entity_id in ids:
        for prop in tb.fetch_properties(entity_id):
            tgt = prop.get("target_id")
            if not tgt:
                continue
            # Get all @type values for the target
            types = {
                p["value"]
                for p in tb.fetch_properties(tgt)
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
    
    # TODO 
    # We need to bring text to the to the table

    # Expand and rebuild table
    tb.expand_properties(table, candidates)
    tb.entity_table(table)

    # Read data from DB
    with sqlite3.connect(database) as conn:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)

    return prop_types, df








def clean_dataframe(DataFrame, config_file):
    
    with open(config_file) as f:
        config = json.load(f)
    
    result = [(key, v) for key, val in type_map.items() for v in val]
    combined_props = []

    for prop_name, target_type in result:
        # Only proceed if target_type exists in config["tables"]
        if target_type in config["tables"]:
            subprops = config["tables"][target_type]["properties"]
            for subprop in subprops:
                combined_props.append(f"{prop_name}_{subprop}")

    # print all combined results
    combined_props
    

