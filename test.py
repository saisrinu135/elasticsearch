from settings import get_settings
from services import get_client
from pprint import pprint


settings = get_settings()



def get_data():
    client = get_client()
    index_name =  settings.index_name

    if not client.indices.exists(index=index_name):
        print("HI")
        return {"status": "error", "message": f"Index '{index_name}' does not exist."}
    
    try:
        response = client.search(
            index=index_name,
            query={
                "match": {
                    "authors": "Martinez"
                }
            }
        )
        print("Count:", response['hits']['total']['value'])
        for hit in response['hits']['hits']:
            pprint(hit['_source'])
    except Exception as e:
        return {"status": "error", "message": f"Search query failed: {e}"}


if __name__ == '__main__':
    get_data()