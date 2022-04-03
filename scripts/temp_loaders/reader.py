from hdfs import InsecureClient
import json 

client = InsecureClient('http://10.4.41.44:9870', user='bdm')

# Loading a file in memory.
def load_file_in_memory(path):
    with client.read(path) as reader:
        features = reader.read()
    return features

# # Directly deserializing a JSON object.
# with client.read('model.json', encoding='utf-8') as reader:
#   from json import load
#   model = load(reader)

kur = load_file_in_memory('/user/bdm/triphawk/data/attractions/20220402/Barceloneta.json')
print(kur)