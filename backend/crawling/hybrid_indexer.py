import os, json, hashlib, datetime, sys
from elasticsearch import Elasticsearch, helpers

# Configuración
es = Elasticsearch(hosts=["http://localhost:9200"])
BASE = "normativa"
CANON_INDEX = f"{BASE}_canon"
INPUT_DIR = "./normativa"

def today_str():
    return datetime.date.today().isoformat()

CRAWL_VERSION = sys.argv[1] if len(sys.argv) > 1 else today_str()
RAW_INDEX = f"{BASE}-{CRAWL_VERSION}"

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def index_raw_snapshots():
    actions = []
    now_iso = datetime.datetime.utcnow().isoformat()
    count = 0

    for fn in os.listdir(INPUT_DIR):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(INPUT_DIR, fn)
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)

        url = doc.get("url", fn)
        texto = doc.get("texto", "")
        chash = sha256_text(texto)

        doc["_meta"] = {
            "last_crawled_at": now_iso,
            "crawl_version": CRAWL_VERSION,
            "content_hash": chash
        }

        actions.append({
            "_op_type": "index",
            "_index": RAW_INDEX,
            "_id": url,
            "_source": doc
        })
        count += 1

    if actions:
        helpers.bulk(es, actions)
    return count

def upsert_canonical_with_history():
    now_iso = datetime.datetime.utcnow().isoformat()
    actions = []

    for fn in os.listdir(INPUT_DIR):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(INPUT_DIR, fn)
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)

        url = doc.get("url", fn)
        texto = doc.get("texto", "")
        titulo = doc.get("titulo", "")
        chash = sha256_text(texto)

        script = {
            "source": """
                if (ctx._source.version_history == null) {
                    ctx._source.version_history = [];
                }
                if (ctx._source.current == null) {
                    ctx._source.current = params.new_doc;
                    ctx._source._meta = params.meta;
                    ctx._source.version_history.add(params.version_entry);
                } else {
                    def prev_hash = ctx._source._meta.content_hash;
                    if (prev_hash != params.meta.content_hash) {
                        ctx._source.version_history.add(params.version_entry);
                    }
                    ctx._source.current = params.new_doc;
                    ctx._source._meta = params.meta;
                }
            """,
            "lang": "painless",
            "params": {
                "new_doc": {"titulo": titulo, "texto": texto, "url": url},
                "meta": {"last_crawled_at": now_iso, "crawl_version": CRAWL_VERSION, "content_hash": chash},
                "version_entry": {"crawled_at": now_iso, "crawl_version": CRAWL_VERSION, "content_hash": chash}
            }
        }

        actions.append({
            "_op_type": "update",
            "_index": CANON_INDEX,
            "_id": url,
            "script": script,
            "upsert": {
                "current": {"titulo": titulo, "texto": texto, "url": url},
                "_meta": {"last_crawled_at": now_iso, "crawl_version": CRAWL_VERSION, "content_hash": chash},
                "version_history": [
                    {"crawled_at": now_iso, "crawl_version": CRAWL_VERSION, "content_hash": chash}
                ]
            }
        })

    if actions:
        helpers.bulk(es, actions)
        return len(actions)
    return 0

def main():
    n_raw = index_raw_snapshots()
    n_canon = upsert_canonical_with_history()

    print(f"[{CRAWL_VERSION}] Snapshots indexados: {n_raw} en '{RAW_INDEX}'")
    print(f"[{CRAWL_VERSION}] Canónicos upsert: {n_canon} en '{CANON_INDEX}'")

if __name__ == "__main__":
    main()