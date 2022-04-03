from scripts.collectors.accommodations import booking as acc
from scripts.temp_loaders import loader
from scripts.temp_loaders import reader
from scripts.perm_loaders import mongo
from datetime import date
import json

current_date = date.today().strftime("%Y%m%d")
HDFS_DIR = f'/user/bdm/triphawk/data/accomodations/{current_date}/'

########
# HDFS # 
########

# fetch the data
accomodations_list = acc.get_accommodations(current_date)

# creates a new directory, each day the date will change
try:
    loader.create_directory_hdfs(HDFS_DIR)
except:
    print("ERROR: Directory already exist")

# go through each object
loader.add_json_to_hdfs(HDFS_DIR, "hotels.json", {"key": accomodations_list})

print("Files uploaded in HDFS")

###########
# MONGODB #
###########

# now once the data is in hdfs, we fetch it
client = mongo.create_connection('10.4.41.44', 27017)
db = mongo.create_database(client, 'triphawk')
coll = mongo.create_collection(db, 'accomodations')

print("Retrieving files from HDFS")


file = reader.load_file_in_memory(f"{HDFS_DIR}/hotels.json")
coll.insert_one(json.loads(file))

print("Files uploaded in MongoDb")

mongo.close_connection(client)