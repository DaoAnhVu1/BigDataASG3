import pandas as pd
from pymongo import MongoClient

s3_url = "https://bigdataasg3-teamhanoi.s3.us-east-1.amazonaws.com/users.csv"

mongo_uri = "mongodb+srv://s3926187:bigdataasg3@bigdataasg3.ih4zw.mongodb.net/?retryWrites=true&w=majority&appName=BigDataASG3"
database_name = "movies"
collection_name = "users"

df = pd.read_csv(s3_url)

client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

data = df.to_dict(orient='records')

collection.insert_many(data)

client.close()

print("Data has been successfully loaded into MongoDB.")