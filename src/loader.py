import os
import zipfile
import requests 
from io import BytesIO
from rocrate_tabular.tabulator import ROCrateTabulator


class loader:
    database = 'corpus.db'
    folder = 'corpus'
    def __init__(self, corpus):
        self.corpus = corpus

    def fetch_zip_folder(self):
        cwd = os.getcwd()
        extract_to = os.path.join(cwd, folder)
        os.makedirs(extract_to, exist_ok=True)
        response = requests.get(self.corpus, stream=True)
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(extract_to)
    
    def fetch_data(sellf):
        pass

