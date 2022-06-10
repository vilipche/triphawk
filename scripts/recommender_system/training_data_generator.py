# training data: [(A,B,R)] which represents user A gave business B a rating R
import pymongo
import mysql.connector
import numpy as np
import random
import pickle

business_type = 'restaurant'
MONGO_COLL = business_type + "_new"
MYSQL_TB = business_type + "_training_data"
PICKLE_FILE = business_type + ".pkl"
print("Start generating training data for " + business_type)

# Build mongo connection
client = pymongo.MongoClient('10.4.41.44', 27017)
db = client["triphawk"]
coll = db[MONGO_COLL]
print("Connected to mongo")

# Retrieve all business ids
b_real_id_list=[]
cursor = coll.find({})
for doc in cursor:
    data = doc['data']
    for business in data:
        b_real_id_list.append(business['id'])
client.close()
print("Retrieve %d business ids" % len(b_real_id_list))
# As the ML model requires integer id, 
# we assume the integer id of business is its index in the b_real_id_list.
# The mapping is saved and will be used in the prediction
b_id_list = [id for id in range(len(b_real_id_list))]
b_mapping = {b_id_list[i]:b_real_id_list[i] for i in range(len(b_real_id_list))}
with open('pickle/'+ PICKLE_FILE, 'wb') as f:
    pickle.dump(b_mapping, f)

# Prepare MySQL
db = mysql.connector.connect(
  host="localhost",
  user="bdm",
  password="bdma",
  database="triphawk"
)
cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS `' + MYSQL_TB + '` (u_id INT, b_id INT, r INT)')
cursor.execute('TRUNCATE TABLE ' +  MYSQL_TB)

# Generate training data and store it into MySQL
user_num = 10000
user_id_start = 10000
rating_per_user = 5
mu = 3.6
sigma = 0.5

user_id_list = [id for id in range(user_id_start, user_id_start + user_num)]
for u_id in user_id_list:
    print('generateing (user, business, rating) for uid:', u_id)
    b_ids = random.sample(b_id_list, k=rating_per_user)
    ratings = np.random.normal(mu, sigma, rating_per_user)
    ratings = [int(r) for r in ratings]
    ratings = [r if r>=0 else 0 for r in ratings]
    ratings = [r if r<=5 else 5 for r in ratings]
    ABR = [(u_id, b_ids[i], ratings[i]) for i in range(rating_per_user)]

    sql = "INSERT INTO " + MYSQL_TB + " (u_id, b_id, r) VALUES (%s, %s, %s)"
    cursor.executemany(sql, ABR)
    db.commit()
    
db.close()

print(business_type + " done!")