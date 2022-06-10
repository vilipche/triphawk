from scripts.formatter.business import lineage_restaurant, lineage_bar
from scripts.formatter.mongo_spark import *

spark = configure_session()

# ######
# ### RESTAURANTS
# #####
restaurantRDD = read_from_mongo(spark, "triphawk.restaurant")
print("Read")
finalRestaurantRDD = lineage_restaurant(restaurantRDD)
print("Write")
write_to_mongo(finalRestaurantRDD, "triphawk.restaurant_formatted")
print("Done")


######
### Bars
#####
barRDD = read_from_mongo(spark, "triphawk.bar")
print("read")
finalBarRDD = lineage_bar(barRDD)
print("write")
write_to_mongo(finalBarRDD, "triphawk.bar_formatted")
print("done")