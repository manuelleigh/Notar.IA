import datetime
import copy
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=["http://localhost:9200"])

BASE = "normativa"
CRAWL_DATE = datetime.date.today().isoformat()  # YYYY-MM-DD
RAW_INDEX = f"{BASE}-{CRAWL_DATE}"
CANON_INDEX = f"{BASE}_canon"
ALIAS_NAME = f"{BASE}_current"

SPANISH_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "spanish_stop": {"type": "stop", "stopwords": "_spanish_"},
                "spanish_stemmer": {"type": "stemmer", "language": "light_spanish"}
            },
            "analyzer": {
                "spanish_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "spanish_stop", "spanish_stemmer"]
                }
            }
        }
    }
}

RAW_MAPPINGS = {
    "mappings": {
        "properties": {
            "titulo": {"type": "text", "analyzer": "spanish_analyzer"},
            "texto": {"type": "text", "analyzer": "spanish_analyzer"},
            "url": {"type": "keyword"},
            "_meta": {
                "properties": {
                    "last_crawled_at": {"type": "date"},
                    "crawl_version": {"type": "keyword"},
                    "content_hash": {"type": "keyword"}
                }
            }
        }
    }
}

CANON_MAPPINGS = {
    "mappings": {
        "properties": {
            "current": {
                "type": "object",
                "properties": {
                    "titulo": {"type": "text", "analyzer": "spanish_analyzer"},
                    "texto": {"type": "text", "analyzer": "spanish_analyzer"},
                    "url": {"type": "keyword"}
                }
            },
            "_meta": {
                "type": "object",
                "properties": {
                    "last_crawled_at": {"type": "date"},
                    "crawl_version": {"type": "keyword"},
                    "content_hash": {"type": "keyword"}
                }
            },
            "version_history": {
                "type": "nested",
                "properties": {
                    "crawled_at": {"type": "date"},
                    "crawl_version": {"type": "keyword"},
                    "content_hash": {"type": "keyword"}
                }
            }
        }
    }
}

def ensure_index(name, body):
    if not es.indices.exists(index=name):
        es.indices.create(index=name, body=body)
        print(f"✔ Índice creado: {name}")
    else:
        print(f"ℹ Índice ya existe: {name}")

def move_alias(alias, index):
    actions = []
    try:
        current = es.indices.get_alias(name=alias)
        for idx in current.keys():
            actions.append({"remove": {"index": idx, "alias": alias}})
    except:
        pass
    actions.append({"add": {"index": index, "alias": alias}})
    es.indices.update_aliases(body={"actions": actions})
    print(f"Alias '{alias}' ahora apunta a: {index}")

def main():
    raw_body = copy.deepcopy(SPANISH_SETTINGS)
    raw_body.update(RAW_MAPPINGS)
    ensure_index(RAW_INDEX, raw_body)
    move_alias(ALIAS_NAME, RAW_INDEX)
    canon_body = copy.deepcopy(SPANISH_SETTINGS)
    canon_body.update(CANON_MAPPINGS)
    ensure_index(CANON_INDEX, canon_body)

    print("\nEstructura lista:")
    print(f"   - RAW (por fecha): {RAW_INDEX}")
    print(f"   - Alias actual:    {ALIAS_NAME} -> {RAW_INDEX}")
    print(f"   - Canónico:        {CANON_INDEX}")

if __name__ == "__main__":
    main()