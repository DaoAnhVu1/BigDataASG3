import ollama
import chromadb

chroma_client = chromadb.HttpClient(host='localhost', port=8000)
collection = chroma_client.get_or_create_collection(name="docs")

# prompt = "I want to learn something about the past and history. Something truthful ? Which movies should I watch, recommend some"
prompt = "I want to see some action and intense movies ? Which one should I watch ?"
# prompt = "I and my partner want to see some romantic movies at the weekend, recommend"

response = ollama.embeddings(
    prompt=prompt,
    model="mxbai-embed-large"
)

print("Getting Data")

results = collection.query(
    query_embeddings=[response["embedding"]],
    n_results=5
)
data = results['documents'][0][0]

final_prompt = f"Using this data: {data}. Respond to this prompt: {prompt}, give at most 3 recommendations, some description and reason why. Pick 3 randoms from the data. Just answer without any follow up."

print("Generating response ...")

output = ollama.generate(
    model="llama3.1:8b",
    prompt=final_prompt,
    stream=True
)

for chunk in output:
    print(chunk['response'], end="", flush=True)
