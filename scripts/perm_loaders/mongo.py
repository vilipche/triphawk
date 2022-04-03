from pymongo import MongoClient
client = MongoClient('10.4.41.44', 27017)

def create_connection(host, port):
    return MongoClient(str(host), int(port))

def close_connection(client):
    client.close()

def create_database(client, db_name):
    return client[db_name]

def create_collection(db, collection_name):
    return db[collection_name]

def list_collections(db):
    return db.list_collection_names()