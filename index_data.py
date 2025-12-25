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
    

def _create_index(client: Elasticsearch, index_name: str = settings.index_name, use_n_gram_tokenizer: bool = False):
    try:
        if not _check_index_exists(client=client, index_name=index_name):
            tokenizer = 'standard' if not use_n_gram_tokenizer else 'ngram_tokenizer'

            body = {
                    "settings": {
                        "analysis": {
                            "analyzer": {
                                "default": {
                                    "type": "custom",
                                    "tokenizer": tokenizer
                                }
                            },
                            "tokenizer": {
                                "ngram_tokenizer":{
                                    "type": "edge_ngram",
                                    "min_gram": 1,
                                    "max_gram": 30,
                                    "token_chars": ['letter', 'digit']
                                }
                            }
                        }
                    }
                }

            return client.indices.create(
                index=index_name,
                body=body
            )

    except Exception as e:
        raise ConnectionError(f"Error creating index: {e}")
    

def _insert_documents(client: Elasticsearch, index_name: str, documents: list[dict]):
    try:

        documents_to_be_indexed = []
        for doc in documents:
            documents_to_be_indexed.append({'index': {'_index': index_name}})
            documents_to_be_indexed.append(doc)
        
        return client.bulk(body=documents_to_be_indexed)
    
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


def index_data(index_name: str = settings.index_name, use_n_gram_tokenizer: bool = False):
    try:
        client = get_client()
        index_name = settings.ngram_index if use_n_gram_tokenizer else settings.index_name
        _create_index(client=client, index_name=index_name, use_n_gram_tokenizer=use_n_gram_tokenizer)
        documents = read_data('data/apod.json')
        _insert_documents(client=client, index_name=index_name, documents=documents)
        pprint(f"Data indexing completed in {index_name}")
    
    except Exception as e:
        print("Error in indexing data:", e)
    

if __name__ == '__main__':
    index_data(use_n_gram_tokenizer=True)