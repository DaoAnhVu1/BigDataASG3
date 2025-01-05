import pandas as pd
import pymongo

s3_url = "https://bigdataasg3-teamhanoi.s3.us-east-1.amazonaws.com/imdb_top_1000.csv"

mongo_uri = "mongodb+srv://s3926187:bigdataasg3@bigdataasg3.ih4zw.mongodb.net/?retryWrites=true&w=majority&appName=BigDataASG3"
database_name = "movies"
collection_name = "movies"

df = pd.read_csv(s3_url)

if "No_of_Votes" in df.columns:
    df.drop(columns=["No_of_Votes"], inplace=True)

def clean_gross(value):
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except ValueError:
        return None

df["Gross"] = df["Gross"].apply(clean_gross)

df["Genre"] = df["Genre"].str.replace('[",]', '', regex=True).str.split()

df["Stars"] = df[["Star1", "Star2", "Star3", "Star4"]].apply(lambda x: [star for star in x if pd.notna(star)], axis=1)
df.drop(columns=["Star1", "Star2", "Star3", "Star4"], inplace=True)

df.insert(0, "id", range(1, len(df) + 1))

print(df.head())

client = pymongo.MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

collection.delete_many({})

records = df.to_dict(orient="records")
collection.insert_many(records)

print("Data transformation, addition of id column, and loading to MongoDB complete.")