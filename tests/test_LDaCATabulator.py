from rocrate_tabular.tabulator import ROCrateTabulator
from src.tabulator import LDaCATabulator
import pytest
import zipfile
from unittest.mock import MagicMock, patch
from pathlib import Path
from src.utils import unzip_corpus
import pandas as pd

def test_get_text():
    pass