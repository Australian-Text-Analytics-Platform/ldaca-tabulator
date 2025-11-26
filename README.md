# LDaCA Tabulator
A Python toolkit for loading, cleaning, and exploring LDaCA RO-Crate corpora.

---

## Overview

**LDaCA Tabulator** provides a simple, consistent interface for working with  
**LDaCA RO-Crate corpora**. It automates:

- Downloading a ZIP corpus from URL  
- Extracting the RO-Crate folder 
- Building an SQLite database from metadata  
- Loading entity tables (RepositoryObject, Person, Organization, Speaker, etc.)  
- Returning analysis-ready Pandas DataFrames  

It wraps `ROCrateTabulator` and adds LDaCA-specific logic.

---

## Installation (Using `uv`)

Since the package is not published yet, you install it locally or via Git.

### 📍 Option 1 — Clone locally

```bash
git clone https://github.com/Australian-Text-Analytics-Platform/ldaca-loader.git
cd ldaca-loader
uv sync
```
