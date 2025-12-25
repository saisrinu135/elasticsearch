from elasticsearch import Elasticsearch
from services import get_client
from settings import get_settings
from pprint import pprint

import json

settings = get_settings()




def _check_index_exists(client: Elasticsearch, index_name: str) -> bool:
    try:
        return client.indices.exists(index=index_name)
    except Exception as e:
        raise ConnectionError(f"Error checking if index exists: {e}")


def create_pipeline(client: Elasticsearch, pipeline_id: str):
    try:
        pipeline_body = {
            "description": "parses the text and removed the html content",
            "processors": [
                {
                    "html_strip": {
                        "field": "explanation"
                    }
                },
                {
                    "html_strip": {
                        "field": "explanation"
                    }
                }
            ]
        }

        return client.ingest.put_pipeline(id=pipeline_id, body=pipeline_body)
    
    except Exception as e:
        print(e)
    

def _create_index(client: Elasticsearch, index_name: str = settings.index_name):
    try:
        if not _check_index_exists(client=client, index_name=index_name):
            return client.indices.create(index=index_name)

    except Exception as e:
        raise ConnectionError(f"Error creating index: {e}")
    

def _insert_documents(client: Elasticsearch, index_name: str, documents: list[dict], pipeline_id: str):
    try:

        documents_to_be_indexed = []
        for doc in documents:
            documents_to_be_indexed.append({'index': {'_index': index_name}})
            documents_to_be_indexed.append(doc)
        
        return client.bulk(body=documents_to_be_indexed, pipeline=pipeline_id)
    
    except Exception as e:
        print("Error inserting documents:", e)
        raise ConnectionError(f"Error inserting documents: {e}")

def read_data(file_path: str) -> list[dict]:
    try:
        with open(file_path, 'r') as file:
            documents = json.load(file)
        return documents
    except Exception as e:
        print("Error reading data from file:", e)
        return []


def index_data(index_name: str, pipeline_id: str):
    try:
        client = get_client()
        _create_index(client=client, index_name=index_name)
        documents = read_data('data/apod.json')
        _insert_documents(client=client, index_name=index_name, documents=documents, pipeline_id=pipeline_id)
        pprint(f"Data indexing completed in {index_name}")
    
    except Exception as e:
        print("Error in indexing data:", e)
    

if __name__ == '__main__':
    pipeline_id = "apod_pipeline"
    index_data(index_name=settings.pipeline_index, pipeline_id=[pipeline_id])