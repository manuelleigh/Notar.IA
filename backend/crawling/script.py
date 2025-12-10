import requests
from bs4 import BeautifulSoup
import os
import json
from urllib.parse import urljoin, urlparse

OUTPUT_DIR = "./crawling/data/gob_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE_URL = "https://www.gob.pe/busquedas"
visited = set()

def crawl(url, depth=1, max_depth=2):
    if depth > max_depth or url in visited:
        return
    visited.add(url)
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error al acceder a {url}")
            return
        soup = BeautifulSoup(response.content, "html.parser")
        text = " ".join([t.get_text(strip=True) for t in soup.find_all("p")])
        title = url.split("/")[-1] or "index"
        txt_path = os.path.join(OUTPUT_DIR, f"{title}.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
        json_path = os.path.join(OUTPUT_DIR, f"{title}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({"titulo": title, "texto": text, "url": url}, f, ensure_ascii=False, indent=2)
        print(f"Guardado: {url}")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            full_url = urljoin(url, href)
            if urlparse(full_url).netloc == urlparse(BASE_URL).netloc:
                crawl(full_url, depth + 1, max_depth)
    except Exception as e:
        print(f"Error procesando {url}: {e}")

crawl(BASE_URL)
