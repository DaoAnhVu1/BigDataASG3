import pandas as pd
from pymongo import MongoClient

df1 = pd.read_csv('https://bigdataasg3-teamhanoi.s3.us-east-1.amazonaws.com/movie_genres.csv')
df2 = pd.read_csv('https://bigdataasg3-teamhanoi.s3.us-east-1.amazonaws.com/imdb_top_1000.csv')

df2['cleaned_genres'] = df2['Genre'].str.replace('"', '').str.replace(',', '').str.lower()

client = MongoClient('mongodb+srv://s3926187:bigdataasg3@bigdataasg3.ih4zw.mongodb.net/?retryWrites=true&w=majority&appName=BigDataASG3')
db = client['movies']
collection = db['recommendation_db']

for idx, row in df1.iterrows():
    genre_in_df1 = row['genre'].lower()

    matching_titles = df2[df2['cleaned_genres'].str.contains(genre_in_df1)]['Series_Title']

    recommended_movies = matching_titles.tolist()
   
    document = {
        "genre": row['genre'],
        "description": row['description'],
        "recommended_movies": recommended_movies
    }
    
    collection.insert_one(document)

client.close()

print("Data inserted successfully.")
