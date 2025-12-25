from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from services import get_client
from settings import get_settings
from pprint import pprint
import utils

from pydantic import BaseModel
import torch

from sentence_transformers import SentenceTransformer


class ResponseSchema(BaseModel):
    status: str = "success"
    status_code: int = 200
    data: list[dict]


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

settings = get_settings()

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = SentenceTransformer('all-MiniLM-L6-v2').to(device)


@app.get(
    '/v1/search',
    description='Search for a query in the database'
)
def search(query: str | None = None, skip: int = 0, limit: int = 10):
    if not query:
        return {"data": []}

    index_name = settings.ngram_index

    try:
        client = get_client()

        response = client.search(
            index=index_name,
            body={
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

        results = [hit.get('_source')
                   for hit in response.get('hits', {}).get('hits', [])]

        return {"data": results}

    except Exception as e:
        print("Error getting data:", e)
        raise e


@app.get(
    '/calculate_hits_per_year',
    description='Calculate number of hits per year',
    status_code=200
)
def calculate_hits_per_year(searcy_query: str):

    query = {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": searcy_query,
                        "fields": ["title", "explanation", "authors"]
                    }
                }
            ]
        }
    }

    aggs = {
        "docs_per_year": {
            "date_histogram": {
                "field": "date",
                "calendar_interval": "year",
                "format": "yyyy"
            }
        }
    }

    client = get_client()

    response = client.search(
        index=settings.index_name,
        body={
            "query": query,
            "aggs": aggs
        },
        filter_path=['aggregations.docs_per_year.buckets']
    )

    results = utils.format_docs_per_year(response)

    return {'status': "success", "data": results}


@app.get(
    '/v2/search',
    description="search with optional filters",
    status_code=200
)
def search_v2(
    search_query: str | None = None,
    year: int | None = None,
    skip: int = 0,
    size: int = 10
):
    """
    Docstring for search_v2

    :param search_query: Description
    :type search_query: str | None
    :param year: Description
    :type year: int | None
    :param skip: Description
    :type skip: int
    :param limit: Description
    :type limit: int
    """

    client = get_client()

    if not search_query:
        return {"data": [], "status": "success"}

    query = {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": search_query,
                        "fields": ["title", "explanation", "authors"]
                    }
                }
            ]
        }
    }

    if year:
        query['bool']['filter'] = [
            {
                "range": {
                    "date": {
                        "gte": f"{year}-01-01",
                        "lte": f"{year}-12-31",
                        "format": "YYYY-MM-DD"
                    }
                }
            }
        ]
    
    response = client.search(
        index=settings.vector_index,
        body={
            "query": query,
            "from": skip,
            "size": size
        },
        filter_path=[
            'hits.hits._source',
            'hits.total.value'
        ]
    )

    total_hits = response['hits']['total']['value']
    total_pages = (total_hits + size - 1) // size

    results = [hit.get('_source') for hit in response.get('hits', {}).get('hits', [])]


    return (
        {
            "status": "success",
            "status_code": 200,
            "data": results,
            "total_hits": total_hits,
            "max_pages": total_pages,
            "count": len(results)
        }
    )


@app.get(
    '/v2/vector-search',
    description="search with optional filters",
    status_code=200
)
def vector_search(
    search_query: str | None = None,
    year: int | None = None,
    skip: int = 0,
    size: int = 10
):
    """
    Docstring for search_v2

    :param search_query: Description
    :type search_query: str | None
    :param year: Description
    :type year: int | None
    :param skip: Description
    :type skip: int
    :param limit: Description
    :type limit: int
    """

    client = get_client()

    if not search_query:
        return {"data": [], "status": "success"}

    query = {
        "bool": {
            "must": [
                {
                    "knn": {
                        "field": "embedding",
                        "query_vector": model.encode(search_query).tolist(),
                        "k": size,
                        "num_candidates": 100
                    }
                }
            ]
        }
    }

    if year:
        query['bool']['filter'] = [
            {
                "range": {
                    "date": {
                        "gte": f"{year}-01-01",
                        "lte": f"{year}-12-31",
                        "format": "YYYY-MM-DD"
                    }
                }
            }
        ]
    
    response = client.search(
        index=settings.vector_index,
        body={
            "query": query,
            "from": skip,
            "size": size
        },
        filter_path=[
            'hits.hits._source',
            'hits.total.value'
        ]
    )

    total_hits = response['hits']['total']['value']
    total_pages = (total_hits + size - 1) // size

    results = [hit.get('_source').remove('embedding') for hit in response.get('hits', {}).get('hits', [])]


    return (
        {
            "status": "success",
            "status_code": 200,
            "data": results,
            "total_hits": total_hits,
            "max_pages": total_pages,
            "count": len(results)
        }
    )
