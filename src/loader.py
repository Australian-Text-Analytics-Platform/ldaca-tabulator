
import zipfile
import requests 
from io import BytesIO
from rocrate_tabular.tabulator import ROCrateTabulator
from collections import defaultdict
import itertools
from pathlib import Path
from typing import Tuple
import shutil

def unzip_corpus(
    zip_url: str,
    tb,                               # an instance: tb = ROCrateTabulator()
    folder_name: str | None = None,   # optional; defaults to URL stem
    db_name: str | None = None,       # optional; defaults to <folder_name>.db
    overwrite: bool = False,) -> Tuple[Path, Path]:
    
    # Resolve target names/paths
    if folder_name is None:
        # If URL ends with "...something.zip", Path(...).stem -> "something"
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
            # Folder already present; skip re-download/re-extract
            extract_to.mkdir(parents=True, exist_ok=True)
    else:
        extract_to.mkdir(parents=True, exist_ok=True)

        # Download (in-memory) and extract
        resp = requests.get(zip_url, stream=True)
        resp.raise_for_status()
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            zf.extractall(extract_to)

    # Build (or connect) DB via the tabulator’s convenience method
    tb.crate_to_db(str(extract_to), str(database))

    return database, extract_to


def expand_for_entity_types(
    tabulator: ROCrateTabulator,
    table: str,
    #target_types: list[str],
    sample: int | None = None,
    verbose: bool = True,):

    # prepare tables
    tabulator.infer_config() # Not sure if I need this here. Need to check
    target_types = list(tabulator.config["potential_tables"])

    if "Language" in target_types:
        target_types.remove("Language")

    tabulator.use_tables(target_types)

    config = tabulator.config["tables"][table]
    if not config.get("all_props"):
        tabulator.entity_table(table)

    ids = tabulator.fetch_ids(table)
    if sample:
        ids = itertools.islice(ids, sample)

    prop_types = defaultdict(set)

    # Scan each entity’s properties
    for entity_id in ids:
        for prop in tabulator.fetch_properties(entity_id):
            tgt = prop.get("target_id")
            if not tgt:
                continue
            # Get all @type values for the target
            types = {
                p["value"]
                for p in tabulator.fetch_properties(tgt)
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
    tabulator.expand_properties(table, candidates)
    tabulator.entity_table(table)
    return prop_types


