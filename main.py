from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from services import get_client
from settings import get_settings
from pprint import pprint



app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

settings = get_settings()


@app.get(
    '/search',
    description='Search for a query in the database'
)
def search(query: str | None = None, skip: int = 0, limit: int = 10):
    if not query:
        return {"status": "success", "data": []}
    
    index_name = settings.index_name

    try:
        client = get_client()

        response = client.search(
            index=index_name,
            body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        'fields': ['title', 'explanation', 'authors']
                    }
                },
                "from": skip,
                "size": limit
            },
            filter_path=['hits.hits._source']
        )

        results = [hit['_source'] for hit in response['hits']['hits']]

        return {"status": "success", "data": results}


    except Exception as e:
        print("Error getting data:", e)
        raise e



