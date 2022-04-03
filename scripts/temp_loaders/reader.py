from hdfs import InsecureClient
import json 

client = InsecureClient('http://10.4.41.44:9870', user='bdm')

# Loading a file in memory.
def load_file_in_memory(path):
    with client.read(path) as reader:
        features = reader.read()
    return features

def list_files_in_directory(path):
    # Listing all files inside a directory.
    return client.list(path)
