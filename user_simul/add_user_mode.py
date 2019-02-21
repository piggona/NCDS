import pymongo

def add_user_mode(mode_id,acting_mode):
    user_mode = {"mode_id":mode_id,"acting_mode":acting_mode}
    client = pymongo.MongoClient(host="localhost",port=27017)
    db = client.NCDS
    collection = db["user_acting_mode"]

    collection.insert_one(user_mode)

if __name__ == "__main__":
    mode_id = 3
    acting_mode = {"device":{"device_type":["ios","android"],"prob":[50,50]},"read_preference":{"channel":["1001","1002","1003","1004","1005","1006","1007"],"prob":[50,0,0,0,0,50,0]}}
    add_user_mode(mode_id,acting_mode)