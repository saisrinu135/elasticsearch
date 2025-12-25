from elasticsearch import Elasticsearch
from services import get_client
from settings import get_settings
from pprint import pprint

from sentence_transformers import SentenceTransformer

import json

import torch

settings = get_settings()


def _check_index_exists(client: Elasticsearch, index_name: str) -> bool:
    try:
        return client.indices.exists(index=index_name)
    except Exception as e:
        raise ConnectionError(f"Error checking if index exists: {e}")


def _create_index(client: Elasticsearch, index_name: str = settings.index_name, use_n_gram_tokenizer: bool = False):
    try:
        if not _check_index_exists(client=client, index_name=index_name):
            return client.indices.create(
                index=index_name,
                body={
                    "mappings": {
                        "properties": {
                            "embedding": {
                                "type": "dense_vector",
                                "dims": 768
                            }
                        }
                    }
                }
            )
        print("Done")

    except Exception as e:
        raise ConnectionError(f"Error creating index: {e}")


def delete_and_create_index(client: Elasticsearch, index_name: str):
    try:
        client.indices.delete(index=index_name, ignore_unavailable=True)
        client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "embedding": {
                            "type": "dense_vector",
                            "dims": 384
                        }
                    }
                }
            }
        )
    
    except Exception as e:
        print("Error deleting and creating index:", e)
        raise ConnectionError(f"Error deleting and creating index: {e}")


def _insert_documents(client: Elasticsearch, index_name: str, documents: list[dict], model):
    try:

        documents_to_be_indexed = []
        for doc in documents:
            documents_to_be_indexed.append({'index': {'_index': index_name}})

            if model:
                documents_to_be_indexed.append(
                    {
                        **doc,
                        "embedding": model.encode(doc['explanation'])
                    }
                )
            else:
                documents_to_be_indexed.append(doc)
        
        print("Done")

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


def index_data(index_name: str = settings.index_name, use_n_gram_tokenizer: bool = False, model=None):
    try:
        client = get_client()
        delete_and_create_index(client=client, index_name=index_name)
        documents = read_data('data/apod.json')
        _insert_documents(client=client, index_name=index_name,
                          documents=documents, model=model)
        pprint(f"Data indexing completed in {index_name}")

    except Exception as e:
        print("Error in indexing data:", e)


if __name__ == '__main__':

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer('all-MiniLM-L6-v2').to(device)

    index_data(index_name=settings.vector_index,
               use_n_gram_tokenizer=False, model=model)
