import os
import json
import time
import random
import hashlib
import threading
from queue import Queue
from urllib.parse import urlparse, urljoin
from datetime import datetime, date

from playwright.sync_api import sync_playwright
from elasticsearch import Elasticsearch

# ==========================
# CONFIGURACIÓN GENERAL
# ==========================

START_URLS = [
    "https://www.notariado.org/",
    "https://www.gob.pe/busquedas",
]

OUTPUT_HTML_DIR = "./crawling/html_snapshots"
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

CONCURRENCY = 4
MAX_DEPTH_PER_DOMAIN = 2
MAX_PAGES_PER_DOMAIN = 200
MAX_RETRIES = 3

PROXIES = [
    
]
# ==========================
# ELASTICSEARCH
# ==========================

es = Elasticsearch(hosts=["http://localhost:9200"])

BASE = "normativa"

def today_str():
    return date.today().isoformat()

CRAWL_VERSION = today_str()
RAW_INDEX = f"{BASE}-{CRAWL_VERSION}"
CANON_INDEX = f"{BASE}_canon"

# ==========================
# ESTADO GLOBAL (THREAD-SAFE)
# ==========================

url_queue: "Queue[tuple[str,int]]" = Queue()
visited_by_domain = {}
pages_per_domain = {}
state_lock = threading.Lock()

# ==========================
# UTILIDADES
# ==========================

def log_event(event_type: str, **fields):
    """Log JSON listo para ingestar en Elastic (Filebeat, etc)."""
    record = {
        "@timestamp": datetime.utcnow().isoformat(),
        "event": event_type,
        **fields,
    }
    print(json.dumps(record, ensure_ascii=False))

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def clean_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="", query="").geturl()

def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()

def is_asset_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    exts = [
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".zip", ".rar", ".7z", ".tar", ".gz"
    ]
    return any(path.endswith(ext) for ext in exts)

def setup_blocking(page):
    """Bloquea imágenes, vídeo y fuentes para acelerar."""
    page.route("**/*", lambda route: (
        route.abort()
        if route.request.resource_type in ["image", "media", "font"]
        else route.continue_()
    ))

# ==========================
# EXTRACCIÓN Y CARGA DE PÁGINAS
# ==========================

def load_page_universal(page, url: str) -> bool:
    """Carga híbrida con retries y distintos 'wait_until'."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log_event("page_load_attempt", url=url, attempt=attempt)
            page.set_default_navigation_timeout(15000)
            page.set_default_timeout(15000)

            try:
                page.goto(url, wait_until="domcontentloaded")
            except Exception:
                page.goto(url, wait_until="load")

            page.wait_for_selector("body", timeout=8000)
            return True
        except Exception as e:
            log_event("page_load_error", url=url, attempt=attempt, error=str(e))
            time.sleep(1.0 * attempt)

    log_event("page_load_failed", url=url)
    return False

def extract_title(page) -> str:
    try:
        title = page.title()
        if title:
            return title.strip()
    except Exception:
        pass
    return ""

def extract_text(page) -> str:
    try:
        text = page.evaluate("""
            () => {
                const nodes = document.querySelectorAll("p, h1, h2, h3, h4, span, article, section, div");
                return Array.from(nodes)
                    .map(n => n.innerText.trim())
                    .filter(t => t.length > 5)
                    .join(" ");
            }
        """)
        return text or ""
    except Exception:
        return ""

def save_html_snapshot(url: str, html: str):
    try:
        domain = get_domain(url).replace(":", "_")
        path = urlparse(url).path.replace("/", "_").strip("_") or "index"
        fn = f"{domain}__{path}.html"
        with open(os.path.join(OUTPUT_HTML_DIR, fn), "w", encoding="utf-8") as f:
            f.write(html)
    except Exception as e:
        log_event("save_html_error", url=url, error=str(e))

# ==========================
# INDEXACIÓN EN ELASTICSEARCH
# ==========================

def index_document_es(url: str, title: str, text: str, is_asset: bool):
    content = text if text else url
    chash = sha256_text(content)
    now_iso = datetime.utcnow().isoformat()

    raw_doc = {
        "titulo": title,
        "texto": text,
        "url": url,
        "tipo": "asset" if is_asset else "page",
        "_meta": {
            "last_crawled_at": now_iso,
            "crawl_version": CRAWL_VERSION,
            "content_hash": chash,
        }
    }

    es.index(index=RAW_INDEX, id=url, document=raw_doc)
    log_event("index_raw_ok", index=RAW_INDEX, url=url)

    if not is_asset:
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
                "new_doc": {"titulo": title, "texto": text, "url": url},
                "meta": {
                    "last_crawled_at": now_iso,
                    "crawl_version": CRAWL_VERSION,
                    "content_hash": chash
                },
                "version_entry": {
                    "crawled_at": now_iso,
                    "crawl_version": CRAWL_VERSION,
                    "content_hash": chash
                }
            }
        }

        upsert_doc = {
            "current": {"titulo": title, "texto": text, "url": url},
            "_meta": {
                "last_crawled_at": now_iso,
                "crawl_version": CRAWL_VERSION,
                "content_hash": chash
            },
            "version_history": [
                {
                    "crawled_at": now_iso,
                    "crawl_version": CRAWL_VERSION,
                    "content_hash": chash
                }
            ]
        }

        es.update(index=CANON_INDEX, id=url, script=script, upsert=upsert_doc)
        log_event("index_canon_ok", index=CANON_INDEX, url=url)

# ==========================
# CRAWL LÓGICO
# ==========================

def enqueue_url(url: str, depth: int):
    url = clean_url(url)
    domain = get_domain(url)
    if not domain:
        return

    with state_lock:
        if domain not in visited_by_domain:
            visited_by_domain[domain] = set()
            pages_per_domain[domain] = 0

        if url in visited_by_domain[domain]:
            return

        if pages_per_domain[domain] >= MAX_PAGES_PER_DOMAIN:
            return

        visited_by_domain[domain].add(url)
        url_queue.put((url, depth))

def process_page(page, url: str, depth: int):
    domain = get_domain(url)
    log_event("crawl_start", url=url, depth=depth, domain=domain)

    # Assets (PDF, DOC, etc) → se indexan como tal y NO se cargan con Playwright
    if is_asset_url(url):
        log_event("asset_detected", url=url, domain=domain)
        index_document_es(url=url, title="", text="", is_asset=True)
        return

    if not load_page_universal(page, url):
        return

    html = page.content()
    title = extract_title(page)
    text = extract_text(page)

    save_html_snapshot(url, html)

    index_document_es(url=url, title=title, text=text, is_asset=False)

    try:
        links = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
    except Exception:
        links = []

    with state_lock:
        pages_per_domain[domain] += 1
        current_pages = pages_per_domain[domain]

    log_event("page_crawled", url=url, depth=depth, domain=domain, pages=current_pages)

    if depth >= MAX_DEPTH_PER_DOMAIN:
        return

    for link in links:
        try:
            link = clean_url(link)
            if not link.startswith("http"):
                continue
            if get_domain(link) != domain:
                continue
            enqueue_url(link, depth + 1)
        except Exception:
            continue

# ==========================
# WORKER
# ==========================

def worker_thread(worker_id: int):
    proxy = None
    if PROXIES:
        proxy = {"server": random.choice(PROXIES)}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, proxy=proxy)
        page = browser.new_page()
        setup_blocking(page)

        log_event("worker_started", worker_id=worker_id, proxy=proxy)

        while True:
            item = url_queue.get()
            if item is None:
                url_queue.task_done()
                break

            url, depth = item
            try:
                process_page(page, url, depth)
            except Exception as e:
                log_event("worker_error", worker_id=worker_id, url=url, error=str(e))

            url_queue.task_done()

        browser.close()
        log_event("worker_stopped", worker_id=worker_id)

# ==========================
# MAIN
# ==========================

def main():
    log_event("crawl_init", seeds=START_URLS, raw_index=RAW_INDEX, canon_index=CANON_INDEX)

    for url in START_URLS:
        enqueue_url(url, depth=1)

    threads = []
    for i in range(CONCURRENCY):
        t = threading.Thread(target=worker_thread, args=(i,), daemon=True)
        threads.append(t)
        t.start()

    url_queue.join()

    for _ in threads:
        url_queue.put((None, 0))

    for t in threads:
        t.join()

    log_event("crawl_done", domains=list(visited_by_domain.keys()))

if __name__ == "__main__":
    main()