import spacy
from spacy.scorer import Scorer
from spacy.training.example import Example
from spacy.tokens import DocBin
import sys

def evaluate(model_path: str, dev_path: str):
    nlp = spacy.load(model_path)
    db = DocBin().from_disk(dev_path)

    examples = []
    for gold in db.get_docs(nlp.vocab):
        pred = nlp(gold.text)
        examples.append(Example(gold, pred))  # Orden correcto

    scorer = Scorer()
    scores = scorer.score(examples)

    print("=== GLOBAL ===")
    print(f"Precision: {scores['ents_p']:.3f}")
    print(f"Recall:    {scores['ents_r']:.3f}")
    print(f"F1:        {scores['ents_f']:.3f}")

    print("\n=== POR ETIQUETA ===")
    for label, m in scores["ents_per_type"].items():
        print(f"{label}: P={m['p']:.3f} R={m['r']:.3f} F1={m['f']:.3f}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python evaluate_ner.py <model_path> <dev.spacy>")
    else:
        evaluate(sys.argv[1], sys.argv[2])

# python evaluate_ner.py models/model-last training_data/dev.spacy