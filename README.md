# Multinational Retail Data Centralisation  
_Consolidating global sales data to enable data-driven decisions_


## Table of Contents
- [Project Overview](#project-overview)
- [Key Objectives](#key-objectives)
- [Tech Stack](#tech-stack)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)

## Project Overview  
A global retail company was operating with fragmented sales data across multiple systems and geographies. This project builds an end-to-end ETL pipeline to ingest raw sales data, clean and deduplicate the records, and store them in a centralised PostgreSQL database — enabling a **single source of truth** for sales analytics and reporting.

### Key Objectives  
- Centralise disparate sales datasets into one unified schema  
- Improve data quality by removing duplicates and standardising formats  
- Enable analytics teams to query clean, reliable sales data with confidence  
- Build reusable Python modules for extraction, cleaning and loading 


## Tech Stack  
- **Python**: pandas, NumPy, SQLAlchemy  
- **Database**: PostgreSQL  
- **Frameworks/Scripts**: Custom modules for `data_extraction`, `data_cleaning`, `database_utils`, `create_tables`  
- **Storage/Schema**: `db_schema.sql`, `Queries.sql`  
- **Optional**: AWS S3 / Azure Blob Storage integration via `boto3` / `tabula-py` for PDF extraction 


## Installation instructions

```bash

pip install pandas numpy pyyaml sqlalchemy tabula-py requests boto3
```

## Usage instructions

Configure your database credentials in localdb_creds.yaml and ensure sales_data database exists.

```bash
git clone https://github.com/shirleyyg/multinational-retail-data-centralisation696.git

Create the database schema and populate data by running :
python create.py
```

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
