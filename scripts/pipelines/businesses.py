from scripts.temp_loaders import loader
from scripts.temp_loaders import reader
from scripts.collectors.businesses import yelp as buss
from scripts.perm_loaders import mongo
import json

from datetime import date

current_date = date.today().strftime("%Y%m%d")
HDFS_DIR = f'/user/bdm/triphawk/data/businesses/{current_date}/'


# fetch the data
businesses_list = buss.get_businesses(current_date)
print("fetch done")
# creates a new directory, each day the date will change
try:
    loader.create_directory_hdfs(HDFS_DIR)
    # loader.create_directory_hdfs(f"{HDFS_DIR}/bar/")
    # loader.create_directory_hdfs(f"{HDFS_DIR}/restaurant/")
except:
    print("ERROR: Directory already exist")

# go through each object
for buss_obj in businesses_list:
    # loads each object in hdfs
    loader.add_json_to_hdfs(f"{HDFS_DIR}/{buss_obj['type']}/", f"{buss_obj['type']}_{buss_obj['location']}_{current_date}.json", buss_obj)

# now once the data is in hdfs, we fetch it
client = mongo.create_connection('10.4.41.44', 27017)
db = mongo.create_database(client, 'triphawk')

print("Retrieving files from HDFS")

list_of_dir = reader.list_files_in_directory(HDFS_DIR)

for dir_name in list_of_dir:
    coll = mongo.create_collection(db, dir_name)

    list_of_files = reader.list_files_in_directory(f"{HDFS_DIR}/{dir_name}")

    for file_name in list_of_files:
        file = reader.load_file_in_memory(f"{HDFS_DIR}/{dir_name}/{file_name}")
        try:
            coll.insert_one(json.loads(file))
        except:
            print(f"ERROR fetching {dir_name} {file_name} ... skipped")

print("Files uploaded in MongoDb")

mongo.close_connection(client)