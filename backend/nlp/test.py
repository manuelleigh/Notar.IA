import spacy
from spacy.language import Language

@Language.component("postprocess_ents")
def postprocess_ents(doc):
    new_ents = []

    for ent in doc.ents:
        txt = ent.text.strip()

        if txt.lower() == "bbva":
            ent = doc.char_span(ent.start_char, ent.end_char, label="BANCO")
            
        if txt.lower() == "asimismo":
            continue

        if ent is not None:
            new_ents.append(ent)

    doc.ents = new_ents
    return doc

nlp = spacy.load("models/model-last")

ruler = nlp.add_pipe("entity_ruler", after="ner")
ruler.from_disk("entity_ruler_patterns.jsonl")

nlp.add_pipe("postprocess_ents", last=True)

doc = nlp("El arrendatario declara que ha recibido el inmueble ubicado en la Av. Los Libertadores 245, distrito de Miraflores, provincia de Lima, departamento de Lima, en perfecto estado de conservación, comprometiéndose a cancelar mensualmente la suma de S/ 2,850.00 dentro de un plazo máximo de 10 días calendario, siendo aplicable una penalidad de S/ 150.00 por cada día de retraso. Asimismo, la parte arrendadora acepta que los pagos se realizarán mediante depósito en la cuenta bancaria N° 024-8765432198 del Banco BBVA, manteniendo la obligación vigente hasta la fecha de culminación del contrato, establecida para el 30/11/2025.")

print([(ent.text, ent.label_) for ent in doc.ents])