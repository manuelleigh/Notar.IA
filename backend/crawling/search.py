from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=["http://localhost:9200"])

ALIAS = "normativa_current"
QUERY_TERM = "ubigeo"

query = {
    "query": {
        "multi_match": {
            "query": QUERY_TERM,
            "fields": ["titulo^2", "texto"],  # título con más peso
            "type": "best_fields"
        }
    },
    "highlight": {
        "fields": {
            "texto": {"fragment_size": 150, "number_of_fragments": 2}
        }
    },
    "size": 10
}

resp = es.search(index=ALIAS, body=query)

print(f"Total resultados: {resp['hits']['total']['value']}")
print("=" * 50)

for hit in resp["hits"]["hits"]:
    src = hit["_source"]
    titulo = src.get("titulo", "(sin título)")
    url = src.get("url", "(sin url)")
    print(f"- {titulo}\n  URL: {url}")
    if "highlight" in hit and "texto" in hit["highlight"]:
        print("  >>", " ... ".join(hit["highlight"]["texto"]))
    print("-" * 50)
