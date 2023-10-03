import os
import hdfs3
import argparse
import datetime
from opendata_collector import fetch_data_from_openDataBcnSpaces

def populate_temporal_hdfs(hdfs_host, hdfs_port, data_relative_path):
    """
    Populates the HDFS with temporal data from various sources.
    
    Args:
        hdfs_host (str): The hostname of the HDFS.
        hdfs_port (int): The port number for HDFS.
        data_relative_path (str): The relative path to the data directory.
    """
    hdfs = hdfs3.HDFileSystem(hdfs_host, hdfs_port)
    data_path = os.path.join(os.getcwd(), data_relative_path)
    
    # Idealista
    populate_temporal_idealista(data_path, hdfs)
    
    # Lookup Tables
    populate_temporal_lookupTables(data_path, hdfs)
    
    # OpenDataBcn Income
    populate_temporal_openDataBcnIncome(data_path, hdfs)
    
    # OpenDataBcn Leisure
    populate_temporal_openDataBcnLeisure(hdfs)
    
    return

def populate_temporal_idealista(data_path, hdfs):
    """
    Populates the HDFS with data from Idealista.
    
    Args:
        data_path (str): The path to the data directory.
        hdfs (HDFileSystem): The HDFS file system object.
    """
    idealista_path = os.path.join(data_path, 'idealista')
    for file in os.listdir(idealista_path):
        origin_path = os.path.join(idealista_path, file)
        landing_path = os.path.join('/user/bdm/temporal/idealista', file)
        hdfs.put(origin_path, landing_path)
    return

def populate_temporal_lookupTables(data_path, hdfs):
    lookup_path = os.path.join(data_path, 'lookup_tables')
    for file in os.listdir(lookup_path):
        origin_path = os.path.join(lookup_path, file)
        landing_path = os.path.join('/user/bdm/temporal/lookup_tables', file)
        hdfs.put(origin_path, landing_path)
    return

def populate_temporal_openDataBcnIncome(data_path, hdfs):    
    income_path = os.path.join(data_path, 'opendatabcn-income')
    for file in os.listdir(income_path):
        origin_path = os.path.join(income_path, file)
        landing_path = os.path.join('/user/bdm/temporal/opendatabcn-income', file)
        hdfs.put(origin_path, landing_path)
    return

def populate_temporal_openDataBcnLeisure(hdfs): 
    """
    Populates the HDFS with data from OpenDataBarcelona.
    
    Args:
        hdfs (HDFileSystem): The HDFS file system object.
    """
    year = datetime.datetime.now().year
    
    hdfs.touch('/user/bdm/temporal/opendatabcn-leisure/{}_opendatabcn_leisure.json'.format(year))
    with hdfs.open('/user/bdm/temporal/opendatabcn-leisure/{}_opendatabcn_leisure.json'.format(year), 'wb') as hdf:
        data = fetch_data_from_openDataBcnSpaces()
        b = bytes(str(data), 'utf-8')
        hdf.write(b)
    return

def main():
    parser = argparse.ArgumentParser(description='Populate HDFS with temporal data.')
    parser.add_argument('--hdfs_host', default='ninetales.fib.upc.es', type=str, help='The hostname of the HDFS.')
    parser.add_argument('--hdfs_port', default=27000, type=int, help='The port number for HDFS.')
    parser.add_argument('--data_relative_path', default='../data', type=str, help='The relative path to the data directory.')
    args = parser.parse_args()
    
    populate_temporal_hdfs(args.hdfs_host, args.hdfs_port, args.data_relative_path)

if __name__ == "__main__":
    main()
