import argparse
from pathlib import Path
from collections import Counter
import spacy
from spacy.tokens import DocBin
import matplotlib.pyplot as plt
import csv

plt.rcParams["font.family"] = "DejaVu Sans"

def load_docs(spacy_path: str):
    nlp = spacy.blank("es")
    db = DocBin().from_disk(spacy_path)
    return list(db.get_docs(nlp.vocab))

def count_labels(docs):
    counter = Counter()
    for d in docs:
        for e in d.ents:
            counter[e.label_] += 1
    return counter

def save_csv(counter: Counter, path: str):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["label", "count"])
        for k, v in sorted(counter.items()):
            w.writerow([k, v])

def plot_distribution(counter: Counter, out_path: str, title: str = "Distribución de Etiquetas"):
    labels = [k for k, _ in counter.most_common()]
    values = [counter[k] for k in labels]

    plt.figure(figsize=(12, 6))
    bars = plt.bar(labels, values)

    plt.title(title)
    plt.xlabel("Etiqueta")
    plt.ylabel("Frecuencia")

    for b in bars:
        h = b.get_height()
        plt.annotate(f"{int(h)}", (b.get_x() + b.get_width() / 2, h), ha="center", va="bottom")

    plt.tight_layout()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=120)
    plt.close()

def main():
    parser = argparse.ArgumentParser(description="Conteo y gráfico de etiquetas NER")
    parser.add_argument("--file", type=str, required=True)
    parser.add_argument("--out", type=str, required=True)
    args = parser.parse_args()

    docs = load_docs(args.file)
    counter = count_labels(docs)

    print("Conteo de etiquetas:")
    for k, v in sorted(counter.items()):
        print(f"{k}: {v}")

    plot_distribution(counter, args.out, title=f"Distribución — {Path(args.file).name}")

    csv_path = args.out.replace(".png", ".csv")
    save_csv(counter, csv_path)

    print(f"Gráfico guardado en {args.out}")
    print(f"CSV guardado en {csv_path}")

if __name__ == "__main__":
    main()
# python label_distribution.py --file training_data/train.spacy --out reports/label_distribution_train.png