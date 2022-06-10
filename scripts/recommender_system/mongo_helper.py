import pickle

def retrieve_neaby_business_id(client, business_type, location):
    db = client["triphawk"]
    coll = db[business_type + "_formatted"]
    cursor = coll.find({'location':location})
    b_real_id_list = list()
    for doc in cursor:
        data = doc['data']
        for business in data:
            b_real_id_list.append(business['id'])
    mapping = dict()
    with open('scripts/recommender_system/pickle/' + business_type + '.pkl', 'rb') as f:
        mapping = pickle.load(f)
    b_id_list = list()
    for real_id in b_real_id_list:
        # We assume the data consistency
        id = [k for k, v in mapping.items() if v == real_id][0]
        b_id_list.append(id)
    return list(set(b_id_list))

def retrieve_business_info(client, business_type, b_id_list):
    mapping = dict()
    with open('scripts/recommender_system/pickle/' + business_type + '.pkl', 'rb') as f:
        mapping = pickle.load(f)
    # We assume the data consistency
    b_real_id_list = [mapping[id] for id in b_id_list]
    db = client["triphawk"]
    coll = db[business_type + "_formatted"]
    cursor = coll.find()
    b_info = {}
    for doc in cursor:
        data = doc['data']
        for business in data:
            if business['id'] in b_real_id_list:
                k = b_real_id_list.index(business['id'])
                b_info[k] = business
    return b_info



