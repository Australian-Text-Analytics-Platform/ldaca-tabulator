# LDaCA Tabulator
A Python module for loading and fetching LDaCA RO-Crate corpora entities and properties.

---

## Overview

LDaCA Tabulator provides a simple, consistent interface for working with  
LDaCA RO-Crate corpora. It automates:

- Downloading a ZIP corpus from URL  
- Extracting the RO-Crate folder 
- Building an SQLite database from metadata  
- Loading entity tables (RepositoryObject, Person, Organization, Speaker, etc.)  
- Returning analysis-ready Pandas DataFrames  

It uses rocrate-tabular package and adds LDaCA-specific logic.

---

## Installation

Clone the repository and install dependencies using `uv`:

```bash
git clone https://github.com/Australian-Text-Analytics-Platform/ldaca-tabulator.git
cd ldaca-tabulator
uv sync
```

---

## Usage

After installation, you can load an LDaCA corpus and access its tables using `LDaCATabulator`.

### Load a corpus

```python
from src.tabulator import LDaCATabulator

zip_url = (
    "https://data.ldaca.edu.au/api/object/arcp%3A%2F%2Fname%2Chdl10.26180~23961609.zip"
)

ldac = LDaCATabulator(zip_url)
```

### Load the main text table with metadata

```python
text_df = ldac.get_text()
text_df.head()
```
This returns a cleaned DataFrame that includes the text data and metadata for each record in the corpus.

### Load the People table

```python 
people_df = ldac.get_people()
people_df.head()
```

### Running tests 

To run the test suite locally:

1. Clone the repository and install dependencies with `uv`:

```bash
git clone https://github.com/Australian-Text-Analytics-Platform/ldaca-tabulator.git
cd ldaca-tabulator
uv sync
```

2. Activate the virtual environment created by `uv`:
    
```bash
source .venv/bin/activate
```

3. run a test file:

```bash
pytest tests/LDaCATabulator.py
```

