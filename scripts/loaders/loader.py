from hdfs import InsecureClient
import json 


# To view the FS
# http://10.4.41.44:9870/explorer.html#


client = InsecureClient('http://10.4.41.44:9870', user='bdm')

# print(client.list('/user/bdm'))
      
# Data to be written 
dictionary ={ 
  "id": "04", 
  "name": "sunil", 
  "department": "HR"
} 


def from_dict_to_json(dict_var):
    return json.dumps(dict_var, indent = 4) 

def create_directory_hdfs(directory_path):
    client.makedirs(directory_path)
    return True

def add_file_to_hdfs(path, file_name, json_object):
    with client.write(f'{path}{file_name}', encoding='utf-8') as writer:
        json.dump(json_object, writer)
    return True