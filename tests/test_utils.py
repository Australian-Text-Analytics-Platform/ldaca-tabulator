
from src.utils import (load_config,
                       unzip_corpus,
                       load_table_from_db)
from rocrate_tabular.tabulator import ROCrateTabulator

def test_load_config(path = "./tests/crates/minimal/ro-crate-metadata.json"):
    config_file = load_config(path)
    assert '@graph' in config_file

def test_load_table_from_db(url = "./crates/languageFamily.zip"):
    tb = ROCrateTabulator()
    database, extract_to = unzip_corpus(url, tb=tb, overwrite=True)
    load_table_from_db(str(database), "RepositoryObject")

    pass
