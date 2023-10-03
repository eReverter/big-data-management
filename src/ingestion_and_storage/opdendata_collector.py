import urllib.request
import json
import argparse

def fetch_data_from_openDataBcnSpaces():
    """
    Fetches data from OpenDataBCN's specified endpoint and returns it as a dictionary.
    
    OpenDataBCN is Barcelona's open data initiative, providing public access to various datasets.
    This function specifically queries a dataset that contains information on certain spaces.
    
    Returns:
        dict: The data fetched from OpenDataBCN's specified endpoint.
    """
    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=f3721b17-bf9e-4bdd-853c-cb6200e1b442&limit=9999'
    with urllib.request.urlopen(url) as filejob:
        response = filejob.read()
    data = json.loads(response)
    return data['result']['records']

def write_data_to_file(data, file_path):
    """
    Writes the given data to a file at the specified file path in JSON format.
    
    Args:
        data (dict): The data to be written to the file.
        file_path (str): The path where the data file should be written.
    """
    with open(file_path, 'w') as outfile:
        json.dump(data, outfile, indent=4)

def main():
    parser = argparse.ArgumentParser(description='Fetch data from OpenDataBCN and store it to a specified file.')
    parser.add_argument('file_path', type=str, help='The path where the data file should be written.')
    args = parser.parse_args()

    data = fetch_data_from_openDataBcnSpaces()
    write_data_to_file(data, args.file_path)

if __name__ == '__main__':
    main()
