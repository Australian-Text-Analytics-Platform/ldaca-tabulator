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
    overwrite: bool = False, # If already exist. It may give error or use the same corpus. Use default True
):
    """
    Download, extract, and tabulate an RO-Crate corpus into a database.

    This function downloads a ZIP archive from a given URL of LDaCA corpus, extracts its
    contents into a local folder, and converts the extracted RO-Crate dataset
    into a database.

    Parameters
    ----------
    zip_url : str
        URL pointing to the ZIP file containing the RO-Crate corpus.
    tb : ROCrateTabulator
        Instance of ROCrateTabulator used to convert the extracted crate into
        a database via `crate_to_db()`.
    folder_name : str | None, optional
        Name of the directory to extract the corpus into. Defaults to
        `"rocrate"` if not provided.
    db_name : str | None, optional
        Name of the output SQLite database file. Defaults to
        `"{folder_name}.db"` if not provided.
    overwrite : bool, optional
        If `True` and the target extraction folder already exists, it will be
        deleted and recreated before extraction. If `False` and the folder
        already exists, no download or extraction occurs and the existing
        folder is used. Default is `False`.

    Returns
    -------
    tuple[pathlib.Path, pathlib.Path]
        A tuple `(database_path, extract_path)` referring to:
        - `database_path`: Path to the generated SQLite DB.
        - `extract_path` : Path where the ZIP was extracted.

    Notes
    -----
    - If `overwrite=False` and the folder already exists, the ZIP file is
      not downloaded or re-extracted; the existing content is used.
    - `crate_to_db()` is always called, meaning the database will be built or
      updated regardless of extraction behavior.
    """

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
    

def drop_id_columns(df: pd.DataFrame, ids_json_path: str  = "./configs/config-ids.json") -> pd.DataFrame:
    with open(ids_json_path, "r") as f:
        config = json.load(f)

    id_cols = config.get("ids", [])

    # Drop only those that exist in the DataFrame
    cleaned_df = df.drop(columns=[c for c in id_cols if c in df.columns], errors="ignore")

    return cleaned_df
