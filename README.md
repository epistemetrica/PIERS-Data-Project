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