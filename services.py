from elasticsearch import Elasticsearch, ConnectionError
from settings import get_settings
from pprint import pprint


settings = get_settings()


def get_client() -> Elasticsearch:
    try:

        client = Elasticsearch(settings.host_url)
        return client
    
    except Exception as e:
        print("Error creating Elasticsearch client:", e)
        raise ConnectionError("Failed to connect to Elasticsearch")