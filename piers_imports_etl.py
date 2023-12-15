#import libraries
import pandas as pd
import os
import sys
import time
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

def main():
    begin = time.time()

    #ini Dask client
    cluster = LocalCluster(n_workers=10)
    client = Client(cluster)

    #define path
    path = 'data/raw/'
    #get list of data files, ignoring any hidden files in directory 
    datafiles = [file for file in os.listdir(path) if not file.startswith('.')]
    #init filenumber
    filenumber = 1

    #define new col names
    import_colnames_dict = {'Weight': 'weight',
                            'Weight Unit': 'weight_unit',
                            'Quantity': 'qty',
                            'Quantity Type': 'qty_type',
                            'TEUs': 'teus',
                            'Estimated Value': 'value_est',
                            'Arrival Date': 'date_arrival',
                            'Container Piece Count': 'container_piece_count',
                            'Quantity of Commodity Short Description': 'commod_short_desc_qty',
                            'Territory of Origin': 'origin_territory',
                            'Region of Origin': 'origin_region',
                            'Port of Arrival Code': 'arrival_port_code',
                            'Port of Arrival': 'arrival_port_name',
                            'Port of Departure Code': 'departure_port_code',
                            'Port of Departure': 'departure_port_name',
                            'Final Destination': 'dest_final',
                            'Coastal Region': 'coast_region',
                            'Clearing District': 'clearing_district',
                            'Place of Receipt': 'place_receipt',
                            'Shipper': 'shipper_name',
                            'Shipper Address': 'shipper_address',
                            'Consignee': 'consignee_name',
                            'Consignee Address': 'consignee_address',
                            'Notify Party': 'notify_party1_name',
                            'Notify Party Address': 'notify_party1_address',
                            'Also Notify Party': 'notify_party2_name',
                            'Also Notify Party Address': 'notify_party2_address',
                            'Raw Commodity Description': 'commod_desc_raw',
                            'Marks Container Number': 'container_id_marks',
                            'Marks Description': 'marks_desc',
                            'HS Code': 'hs_code',
                            'JOC Code': 'joc_code',
                            'Commodity Short Description': 'commod_short_desc',
                            'Container Number': 'container_ids',
                            'Carrier': 'carrier_name',
                            'SCAC': 'carrier_scac',
                            'Vessel Name': 'vessel_name',
                            'Voyage Number': 'vessel_id',
                            'Pre Carrier': 'precarrier',
                            'IMO Number': 'imo_num',
                            'Inbond Code': 'inbond_code',
                            'Mode of Transport': 'transport_mode',
                            'Bill of Lading Number': 'bol_id'}
    #define dtypes
    import_dtype_dict = {'Weight': 'float64',
                'Weight Unit': 'category',
                'Quantity': 'float64',
                'Quantity Type': 'category',
                'TEUs': 'float64',
                'Estimated Value': 'float64',
                'Arrival Date': 'int64',
                'Container Piece Count': 'int64',
                'Quantity of Commodity Short Description': 'object',
                'Territory of Origin': 'category',
                'Region of Origin': 'category',
                'Port of Arrival Code': 'category',
                'Port of Arrival': 'category',
                'Port of Departure Code': 'category',
                'Port of Departure': 'category',
                'Final Destination': 'category',
                'Coastal Region': 'category',
                'Clearing District': 'category',
                'Place of Receipt': 'category',
                'Shipper': 'object',
                'Shipper Address': 'object',
                'Consignee': 'object',
                'Consignee Address': 'object',
                'Notify Party': 'object',
                'Notify Party Address': 'object',
                'Also Notify Party': 'object',
                'Also Notify Party Address': 'object',
                'Raw Commodity Description': 'object',
                'Marks Container Number': 'object',
                'Marks Description': 'object',
                'HS Code': 'category',
                'JOC Code': 'category',
                'Commodity Short Description': 'object',
                'Container Number': 'object',
                'Carrier': 'category',
                'SCAC': 'category',
                'Vessel Name': 'object',
                'Voyage Number': 'object',
                'Pre Carrier': 'float64',
                'IMO Number': 'float64',
                'Inbond Code': 'float64',
                'Mode of Transport': 'category',
                'Bill of Lading Number': 'object'}
    #define category variable cols
    catcols = ['weight_unit', 'qty_type', 'origin_territory', 'origin_region', 'arrival_port_code', 
            'arrival_port_name', 'departure_port_code', 'departure_port_name', 'dest_final', 'coast_region', 
            'clearing_district', 'place_receipt', 'hs_code', 'joc_code', 'carrier_name', 'carrier_scac', 
            'transport_mode']
    #get col names for reordering
    import_colnames = list(import_colnames_dict.values())


    print('Extracting CSV files...\n', 'Files to process: ', len(datafiles), '\n')

    for file in datafiles:
        #extract from csv to clean dataframes and concat
        start = time.time()
        print('Extracting file {}...'.format(filenumber))
        #read csv with appropriate dtypes
        file_df = dd.read_csv(path+file, dtype=import_dtype_dict, assume_missing=True, sample=1000)
        
        #rename columns
        file_df = file_df.compute().rename(columns=import_colnames_dict)
        #unpack strings to list objects
        file_df.container_ids = file_df.container_ids.str.split()
        file_df.commod_short_desc_qty = file_df.commod_short_desc_qty.str.split(pat=';')
        file_df.commod_short_desc = file_df.commod_short_desc.str.split(pat=',')
        #recast dates to datetime 
        file_df.date_arrival = pd.to_datetime(file_df.date_arrival.astype(str), format='%Y%m%d')
        #reorder columns
        file_df = file_df[import_colnames]
        #concat to imports_df
        if 'imports_df' in locals():
            imports_df = dd.concat([imports_df, file_df])
        else: 
            imports_df = file_df
        #save file_df
        extract = time.time()
        print('File extracted. That took {} sec.'.format(extract-start))
        print('Saving file {} to parquet...'.format(filenumber))
        file_df.to_parquet('data/clean_parquet/'+ file[:-3] + 'parquet')
        del file_df
        end = time.time()
        print('File processing complete.\n', 'Total time for file {}: {} sec \n'.format(filenumber, end-start))
        filenumber += 1

    print('Computing final dataframe...')
    #inspect imports_df
    imports_df.compute().info()

    # Close the Dask client and cluster
    client.close()
    cluster.close()

    final = time.time()
    print('\nETL Complete. Total runtime:', final-begin)


if __name__ == '__main__':
    main()