from pathlib import Path
import shutil
import zipfile
import requests
from io import BytesIO
from rocrate_tabular.tabulator import ROCrateTabulator
import json
import sqlite3
import pandas as pd
from typing import List


def unzip_corpus(
    zip_url: str,
    tb: ROCrateTabulator,
    folder_name: str | None = None,
    db_name: str | None = None,
    overwrite: bool = False,
):

    # Resolve target names/paths
    if folder_name is None:
        folder_name = "rocrate"
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


# loading config file
def load_config(config_path: str):
    with open(config_path) as f:
        config = json.load(f)
    return config


# loading table from database
def load_table_from_db(
    database_path: str,
    table_name: str,
    columns: List[str] | None = None
):
 
    with sqlite3.connect(database_path) as conn:
        if columns:
            cols = ", ".join(f'"{c}"' for c in columns)
        else:
            cols = "*"

        query = f"SELECT {cols} FROM {table_name}"
        return pd.read_sql(query, conn)
    

def auto_ignore_bad_props(tb, action, *args):
    try:
        if action == "use":
            tb.use_tables(*args)
        elif action == "entity":
            tb.entity_table(*args)
        else:
            raise ValueError("Invalid action type. Use 'use' or 'entity'.")
    except Exception as e:
        msg = str(e)
        if "Too many columns" in msg:
            table = args[0] if args else "UnknownTable"
            prop = msg.split("for")[-1].strip()
            print(f"{table}: too many values for '{prop}', ignoring it.")
            conf = tb.config["tables"].get(table, {})
            ignore = conf.get("ignore_props", [])
            if prop not in ignore:
                ignore.append(prop)
            tb.config["tables"][table]["ignore_props"] = ignore
            tb.entity_table(*args)
        elif "already generated" in msg:
            print(f"ℹTable already generated, skipping: {args}")
        elif "not recognised" in msg:
            print(f"Skipping unrecognised table: {args}")
        else:
            raise

