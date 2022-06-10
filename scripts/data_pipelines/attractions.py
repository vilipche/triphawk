from scripts.temp_loaders import loader
from scripts.temp_loaders import reader
from scripts.collectors.attractions import foursquare as attr
from scripts.perm_loaders import mongo
from datetime import date
import json

current_date = date.today().strftime("%Y%m%d")
HDFS_DIR = f'/user/bdm/triphawk/data/attractions/{current_date}/'

########
# HDFS # 
########

# fetch the data
attractions_list = attr.get_attractions(current_date)

# creates a new directory, each day the date will change
try:
    loader.create_directory_hdfs(HDFS_DIR)
except:
    print("ERROR: Directory already exist")

# go through each object
for attr_obj in attractions_list:
    # loads each object in hdfs
    loader.add_json_to_hdfs(HDFS_DIR, f"{attr_obj['location']}.json", attr_obj)

print("Files uploaded in HDFS")

###########
# MONGODB #
###########

# now once the data is in hdfs, we fetch it
client = mongo.create_connection('10.4.41.44', 27017)
db = mongo.create_database(client, 'triphawk')
coll = mongo.create_collection(db, 'attractions')

print("Retrieving files from HDFS")

list_of_files = reader.list_files_in_directory(HDFS_DIR)

for file_name in list_of_files:
    file = reader.load_file_in_memory(f"{HDFS_DIR}{file_name}")
    try:
        coll.insert_one(json.loads(file))
    except:
        print(f"ERROR fetching {file_name} ... skipped")

print("Files uploaded in MongoDb")

mongo.close_connection(client)