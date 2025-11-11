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
import re


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
    


# This function is temporarily added to handle errors when using or entity_table or use_tables 
def auto_ignore_bad_props(tb, action, *args):
    while True:
        try:
            if action == "use":
                tb.use_tables(*args)
            elif action == "entity":
                tb.entity_table(*args)
            else:
                raise ValueError("Invalid action type. Use 'use' or 'entity'.")
            break
        except Exception as e:
            msg = str(e)
            if "Too many columns" in msg:
                table = args[0] if args else "UnknownTable"
                raw_prop = msg.split("for", 1)[-1].strip()
                prop = re.sub(r"_\d+$", "", raw_prop)
                print(f"{table}: too many values for '{prop}', ignoring it.")
                conf = tb.config["tables"].setdefault(table, {})
                ignore = conf.setdefault("ignore_props", [])
                if prop not in ignore:
                    ignore.append(prop)
                continue
            if "already generated" in msg:
                print(f"Table already generated, skipping: {args}")
                break
            if "not recognised" in msg:
                print(f"Skipping unrecognised table: {args}")
                break
            raise
