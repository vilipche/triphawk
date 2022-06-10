from pyspark.sql import SparkSession
import pymongo
from scripts.recommender_system.model_helper import *
from scripts.recommender_system.mongo_helper import *

business_type = 'restaurant'
MYSQL_TB = business_type + '_training_data'
MODEL_PATH = 'trained_model/' + business_type + '_ALS'

######
### Create session and read training data from MySQL
#####
spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("PySpark_MySQL") \
        .config('spark.jars', 'jars/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar') \
        .getOrCreate()

td_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/triphawk") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", MYSQL_TB) \
    .option("user", "bdm").option("password", "bdma") \
    .load()

ratings_df = td_df \
    .selectExpr('u_id as user', 'b_id as item', 'r as rating') \
    .cache()


######
### ML model building
#####
rank = 10
numIterations = 10

print("model training")
model = train_ALS(ratings_df, rank, numIterations)

print("model evaluating")
evaluate_ALS(model, ratings_df.rdd)

print("model saving")
save_model(model, MODEL_PATH, spark.sparkContext)


######
### ML model predicating
#####
request_example = {
    'user_id' : 10001,
    'location' : "Barceloneta",
    'top-k': 10,
}

client = pymongo.MongoClient('10.4.41.44', 27017)
business_id_list = retrieve_neaby_business_id(client, business_type, request_example['location'])
test_rdd = spark.sparkContext.parallelize([(request_example['user_id'], b_id) for b_id in business_id_list])

model = load_model(spark.sparkContext, MODEL_PATH)
predictions = model.predictAll(test_rdd).map(lambda r: ((r[0], r[1]), r[2]))
results = predictions.collect()
results = sorted(results, key=lambda t:t[1], reverse=True) # key needs to be string
top_list = [i[0][1] for i in results[:request_example['top-k']]]
recommend = retrieve_business_info(client, business_type, top_list)

client.close()

print("recommend: ", recommend)
