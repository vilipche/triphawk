from __future__ import barry_as_FLUFL
from pyspark.sql import SparkSession

def configure_session():
    spark = SparkSession \
            .builder \
            .master(f"local[*]") \
            .appName("myApp") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
            
    return spark

def read_from_mongo(spark, collection_name: str):
    rdd = spark.read.format("mongo")\
    .option('uri', f"mongodb://127.0.0.1/{collection_name}") \
    .load() \
    .rdd.cache()
    return rdd

def write_to_mongo(rdd, new_collection_name):
    return rdd.toDF().write\
            .format("mongo")\
            .mode("append")\
            .option('uri', f"mongodb://127.0.0.1/{new_collection_name}").save()