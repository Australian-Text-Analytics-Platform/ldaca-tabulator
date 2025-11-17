
from src.utils import (load_config)


def test_load_config(path = "./tests/crates/minimal/ro-crate-metadata.json"):
    config_file = load_config(path)
    assert '@graph' in config_file

def test_load_table_from_db():
    pass
