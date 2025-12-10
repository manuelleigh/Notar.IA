import spacy
from spacy.tokens import DocBin
from spacy.training import Example
from spacy.scorer import Scorer
import sys, html, json


def highlight_text(text, gold_spans, pred_spans):
    """
    Marca spans:
    - good: correcto
    - missing: está en gold pero no en pred
    - spurious: está en pred pero no en gold
    """
    marks = []

    for start, end, label in gold_spans:
        cls = "good" if (start, end, label) in pred_spans else "missing"
        marks.append((start, end, label, cls))

    for start, end, label in pred_spans:
        if (start, end, label) not in gold_spans:
            marks.append((start, end, label, "spurious"))

    marks.sort(key=lambda x: x[0])

    out = ""
    last_end = 0
    for start, end, label, cls in marks:
        out += html.escape(text[last_end:start])
        span_text = html.escape(text[start:end])
        out += f'<span class="{cls}" data-label="{label}" title="{label}">{span_text}</span>'
        last_end = end
    out += html.escape(text[last_end:])
    return out


def build_dashboard(model_path, dev_path, output="ner_dashboard.html", max_items=300):
    nlp = spacy.load(model_path)
    db = DocBin().from_disk(dev_path)

    examples = []
    errors = []
    scorer = Scorer()

    for i, gold_doc in enumerate(db.get_docs(nlp.vocab)):
        pred_doc = nlp(gold_doc.text)
        ex = Example(gold_doc, pred_doc)
        examples.append(ex)

        gold_spans = {(e.start_char, e.end_char, e.label_) for e in gold_doc.ents}
        pred_spans = {(e.start_char, e.end_char, e.label_) for e in pred_doc.ents}
        missing = gold_spans - pred_spans
        spurious = pred_spans - gold_spans

        if (missing or spurious) and len(errors) < max_items:
            highlighted = highlight_text(gold_doc.text, gold_spans, pred_spans)
            errors.append({
                "id": i,
                "text_html": highlighted,
                "missing": list(missing),
                "spurious": list(spurious),
            })

    scores = scorer.score(examples)
    ents_global = {
        "precision": scores["ents_p"],
        "recall": scores["ents_r"],
        "f1": scores["ents_f"],
    }
    ents_labels = {
        label: {"p": m["p"], "r": m["r"], "f": m["f"]}
        for label, m in scores["ents_per_type"].items()
    }

    all_labels = sorted(ents_labels.keys())

    html_content = f"""
<html>
<head>
<meta charset="utf-8">
<title>Dashboard NER</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
h1 {{ color: #333; }}
.good {{ background-color: #b3ffb3; border-bottom: 2px solid green; }}
.missing {{ background-color: #ffb3b3; border-bottom: 2px solid red; }}
.spurious {{ background-color: #ffd699; border-bottom: 2px solid orange; }}
span {{ padding: 1px 2px; border-radius: 3px; }}
.example {{ margin-bottom: 25px; }}
.text {{ font-size: 15px; margin-bottom: 5px; }}
table {{ border-collapse: collapse; margin-top: 10px; }}
table, th, td {{ border: 1px solid #aaa; padding: 4px 6px; font-size: 13px; }}
.badge {{ display: inline-block; padding: 2px 5px; border-radius: 3px; margin-right: 4px; cursor: pointer; }}
.badge-label {{ background:#eee; }}
.badge-label.active {{ background:#333; color:#fff; }}
</style>
</head>
<body>
<h1>Dashboard de Errores NER</h1>
<p><b>Modelo:</b> {html.escape(model_path)}</p>
<p><b>Corpus dev:</b> {html.escape(dev_path)}</p>

<h2>Métricas globales</h2>
<ul>
  <li>Precision: {ents_global["precision"]:.3f}</li>
  <li>Recall: {ents_global["recall"]:.3f}</li>
  <li>F1: {ents_global["f1"]:.3f}</li>
</ul>

<h2>Métricas por etiqueta</h2>
<table>
<tr><th>Etiqueta</th><th>P</th><th>R</th><th>F1</th></tr>
{"".join(f"<tr><td>{lbl}</td><td>{m['p']:.3f}</td><td>{m['r']:.3f}</td><td>{m['f']:.3f}</td></tr>" for lbl, m in ents_labels.items())}
</table>

<h2>Filtros</h2>
<p>Haz clic en una etiqueta para mostrar solo ejemplos donde falle esa etiqueta (missing o spurious).</p>
<div id="label-filters">
{"".join(f'<span class="badge badge-label" data-label="{lbl}">{lbl}</span>' for lbl in all_labels)}
<span class="badge badge-label" data-label="__all__">[VER TODO]</span>
</div>

<hr>
<h2>Ejemplos con errores (máx {len(errors)})</h2>
<div id="examples"></div>

<script>
const ERRORS = {json.dumps(errors, ensure_ascii=False)};
const container = document.getElementById("examples");
const badges = document.querySelectorAll(".badge-label");

function render(labelFilter=null) {{
  container.innerHTML = "";
  ERRORS.forEach(err => {{
    // si hay filtro, comprobar si el ejemplo contiene esa etiqueta en missing/spurious
    if (labelFilter && labelFilter !== "__all__") {{
      let hasLabel = false;
      [...err.missing, ...err.spurious].forEach(s => {{
        if (s[2] === labelFilter) hasLabel = true;
      }});
      if (!hasLabel) return;
    }}

    const div = document.createElement("div");
    div.className = "example";
    div.innerHTML = `
      <h3>Ejemplo #${{err.id}}</h3>
      <div class="text">${{err.text_html}}</div>
      <table>
        <tr><th>Tipo</th><th>start</th><th>end</th><th>Etiqueta</th></tr>
        ${{err.missing.map(m => `<tr><td>NO DETECTADA</td><td>${{m[0]}}</td><td>${{m[1]}}</td><td>${{m[2]}}</td></tr>`).join("")}}
        ${{err.spurious.map(s => `<tr><td>ERRÓNEA</td><td>${{s[0]}}</td><td>${{s[1]}}</td><td>${{s[2]}}</td></tr>`).join("")}}
      </table>
      <hr>
    `;
    container.appendChild(div);
  }});
}}

badges.forEach(b => {{
  b.addEventListener("click", () => {{
    badges.forEach(x => x.classList.remove("active"));
    b.classList.add("active");
    const lbl = b.getAttribute("data-label");
    render(lbl);
  }});
}});

render("__all__");
</script>

</body>
</html>
"""
    with open(output, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Reporte generado: {output}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python error_report.py <model_path> <dev.spacy> [output.html]")
    else:
        out = sys.argv[3] if len(sys.argv) > 3 else "ner_report.html"
        build_dashboard(sys.argv[1], sys.argv[2], out)
# python error_report.py models/model-last training_data/dev.spacy ner_report.html