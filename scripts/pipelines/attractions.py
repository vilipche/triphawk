from scripts.temp_loaders import loader
from scripts.collectors.attractions import foursquare as attr
from datetime import date

current_date = date.today().strftime("%Y%m%d")
HDFS_DIR = f'/user/bdm/triphawk/data/attractions/{current_date}/'


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


    