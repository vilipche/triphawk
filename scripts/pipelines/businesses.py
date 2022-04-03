from scripts.temp_loaders import loader
from scripts.collectors.businesses import yelp as buss
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

    