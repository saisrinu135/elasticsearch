def format_docs_per_year(response):
    formatted_results = []

    buckets = response.get('aggregations', {}).get('docs_per_year', {}).get('buckets', [])

    for bucket in buckets:
        year = bucket.get('key_as_string')
        doc_count = bucket.get('doc_count')
        formatted_results.append({'year': year, "count": doc_count})
    

    return formatted_results