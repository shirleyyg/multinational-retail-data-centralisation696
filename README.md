# Multinational Retail Data Centralisation

The aim of this project is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. It will help the company to make more data-driven decisions
This project id for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

## Table of Contents
- [Description](#description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)

## Installation instructions

```python
git clone https://github.com/shirleyyg/multinational-retail-data-centralisation696.git

pip install pandas numpy pyyaml sqlalchemy tabula-py requests boto3
```

## Usage instructions


```python
python database_utils.py
python data_extraction.py
python data_cleaning.py
python create_tables.py
```
Create a database in PostgreSQL called sales_data.
Then run create_tables.py file to create tables to sales_data DB


## File structure of the project

MRDC/
├── __pycache__/
├── README.md
├── database_utils.py
├── data_extraction.py
├── data_cleaning.py
├── create_tables.py
├── db_schema.sql
├── Queries.sql
├── api_token.yaml
├── db_creds.yaml
└── localdb_creds.yaml