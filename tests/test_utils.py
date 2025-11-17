from src.utils import load_config

def test_load_config():
    config_file = load_config("./crates/minimal/re-crate-metadata.json")
    assert '@graph' in config_file
