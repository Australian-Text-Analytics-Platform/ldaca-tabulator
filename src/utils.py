from pathlib import Path
import shutil
import zipfile
import requests
from io import BytesIO
from rocrate_tabular.tabulator import ROCrateTabulator
import json


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


