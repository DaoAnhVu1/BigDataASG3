import ollama
import chromadb
import pymongo

mongo_uri = "mongodb+srv://s3926187:bigdataasg3@bigdataasg3.ih4zw.mongodb.net/?retryWrites=true&w=majority&appName=BigDataASG3"
client_mongo = pymongo.MongoClient(mongo_uri)
db = client_mongo["movies"]
collection_mongo = db["recommendation_db"]

documents = list(collection_mongo.find({}))

chroma_client = chromadb.HttpClient(host='localhost', port=8000)
collection = chroma_client.create_collection(name="docs")

for i, doc in enumerate(documents):
    try:
        response = ollama.embeddings(model="mxbai-embed-large", prompt=str(doc["description"]))
        
        embedding = response.get("embedding")
        
        if embedding is not None:
            collection.add(
                ids=[str(i)],
                embeddings=[embedding],
                documents=[str(doc)]
            )
            print("added to collection")
        else:
            print(f"Embedding for document {i} not found.")
    except Exception as e:
        print(f"Error processing document {i}: {e}")

