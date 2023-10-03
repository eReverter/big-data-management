import argparse
from pymongo import MongoClient
import pymongo
from io import StringIO
import pandas as pd
import json
from utils import get_server
import hdfs3

def populate_persistent(server, col_name, data):
    """
    Populates a specified MongoDB collection with given data.
    
    Args:
        server (Server): The server object containing connection details.
        col_name (str): The name of the collection to populate.
        data (list): The data to insert into the collection.
    """
    client = MongoClient('localhost', server.local_bind_port)
    db = client.persistent
    
    try:
        db.validate_collection(col_name)
    except pymongo.errors.OperationFailure:
        collection = db.create_collection(col_name)
            
    db[col_name].insert_many(data)
        
    client.close()
    return

def bytes_to_json(data_bytes, csv=False):
    """
    Converts bytes data to JSON.
    
    Args:
        data_bytes (bytes): The data in bytes format.
        csv (bool): Whether the data is in CSV format.
        
    Returns:
        list: The data converted to JSON.
    """
    if len(data_bytes) <= 10:
        return
    
    s = str(data_bytes, 'utf-8')
    if csv:
        i = StringIO(s)
        df = pd.read_csv(i)
        json_list = json.loads(df.to_json(orient='records'))
    else:
        json_list = json.loads(s)
        
    return json_list    

def populate_persistent_mongo(server, hdfs_host, hdfs_port):
    """
    Populates MongoDB with data from HDFS.
    
    Args:
        server (Server): The server object containing connection details.
        hdfs_host (str): The hostname of the HDFS.
        hdfs_port (int): The port number for HDFS.
    """
    hdfs = hdfs3.HDFileSystem(hdfs_host, hdfs_port)
    
    for directories in hdfs.ls('/user/bdm/temporal/'):
        collection = directories.split('/')[-1]
        print(f'Populating {collection} collection...')
        
        for hdfile in hdfs.ls(f'/user/bdm/temporal/{collection}'):
            bytes_data = hdfs.cat(hdfile)
            if hdfile.split('.')[-1] == 'csv':
                documents = bytes_to_json(bytes_data, csv=True)
            else:
                documents = bytes_to_json(bytes_data)

            if documents:
                populate_persistent(server, collection, documents)
             
    return

def main():
    parser = argparse.ArgumentParser(description='Populate MongoDB with data from HDFS.')
    parser.add_argument('--hdfs_host', required=True, type=str, help='The hostname of the HDFS.')
    parser.add_argument('--hdfs_port', required=True, type=int, help='The port number for HDFS.')
    parser.add_argument('--mongo_host', required=True, type=str, help='The hostname of the MongoDB server.')
    parser.add_argument('--server_user', required=True, type=str, help='The username for the server.')
    parser.add_argument('--server_pwd', required=True, type=str, help='The password for the server.')
    args = parser.parse_args()
    
    server = get_server(args.mongo_host, args.server_user, args.server_pwd)
    populate_persistent_mongo(server, args.hdfs_host, args.hdfs_port)

if __name__ == "__main__":
    main()

