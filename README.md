# PIERS Data Project

The project processes Bill of Lading data from [S&P's Global Trade Analytics Suite](https://www.spglobal.com/marketintelligence/en/mi/products/maritime-global-trade-analytics-suite.html) and provides exploratory analysis. 

## Environment

Scripts are written in Python 3.12 and utilize the following libraries:

- pandas 2.1.3
- numpy 1.26.2
- polars 0.20.1 
- dask 2023.11.0
- matplotlib 3.8.2
- seaborn 0.13.0

## Data

Raw data was downloaded in CSV format from the PIERS BOL page, selecting all fields and downloading six months at a time, and saving in the data/raw folder. 

### Data Files

- data/column_definitions.csv
    - a table of definions for each column, provided by S&P. 
- data/raw/imports/*.csv (122 GB)
    - raw csv files containing import data, as downloaded from S&P global
- data/raw/exports/*.csv (32 GB)
    - raw csv files containing export data, as downloaded from S&P global
- data/piers_imports_complete.parquet (44 GB)
    - [parquet](https://www.databricks.com/glossary/what-is-parquet) file containing a single table of all import data
    - note: the resulting python dataframe is >100 GB and may not be loadable with pandas; [see below regarding the polars library](#using-the-polars-library). 
- data/piers_exports_complete.parquet (7 GB)
    - parquet file containing a single table of all export data
    - note: the resulting python dataframe is ~35 GB

### Pipeline files

- imports_etl.ipynb
    - a jupyter notebook containing the ETL pipeline for import data
- exports_etl.ipynb
    - a jupyther notebook containing the ETL pipeline for export data

## Analysis

This project provides basic exploratory analysis for the PIERS datasets. Model identification, estimation, and inference are handled seperately. 

### Analysis files

- piers_explore.ipynb
    - a jupyter notebook providing exploratory analytics

## Using the Polars library

Polars provides a powerful toolkit for handling large datasets (including larger-than-memory datasets) with an API highly similar to Pandas. See the [Polars cheat sheet](https://franzdiebold.github.io/polars-cheat-sheet/Polars_cheat_sheet.pdf) and [this guide to visulizations with Polars](https://r-brink.medium.com/easy-ways-to-visualise-data-when-using-polars-e2756bc5dd37) for reference. 

*The key concept is to utilize Polars's automatic query optimization and parallelization engines by [chaining expressions](https://docs.pola.rs/user-guide/concepts/expressions/) and calling .collect() at the lastest possible point in your code.* 

Note: When working with larger-than-memory datasets, you may need to call [.collect(streaming=True)](https://docs.pola.rs/user-guide/concepts/streaming/). 

