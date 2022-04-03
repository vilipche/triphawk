from hdfs import InsecureClient
import json 


# To view the FS
# http://10.4.41.44:9870/explorer.html#

# To start hdfs
# ~/BDM_Software/hadoop/sbin/start-dfs.sh

client = InsecureClient('http://10.4.41.44:9870', user='bdm')

def create_directory_hdfs(directory_path):
    client.makedirs(directory_path)
    return True

def add_json_to_hdfs(path, file_name, json_object):
    with client.write(f'{path}{file_name}', encoding='utf-8',permission=777, overwrite=True) as writer:
        json.dump(json_object, writer, ensure_ascii=False)
    return True