from src.utils import (load_config,
                       load_table_from_db,
                       unzip_corpus)
#import sqlite3
from rocrate_tabular.tabulator import ROCrateTabulator

def test_load_config(path = "./tests/crates/minimal/ro-crate-metadata.json"):
    config_file = load_config(path)
    assert '@graph' in config_file

def test_unzip_corpus(url = "./tests/crates/languageFamily.zip", tb = ROCrateTabulator()):
    unzip_corpus(url, tb)
    pass

def test_load_table_from_db():
    pass



