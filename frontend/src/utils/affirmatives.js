export const AFFIRMATIVE_KEYWORDS = [
  "si",
  "sí",
  "sì",
  "sip",
  "sii",
  "see",
  "ok",
  "okay",
  "oki",
  "okey",
  "dale",
  "listo",
  "claro",
  "por supuesto",
  "perfecto",
  "bien",
  "correcto",
  "continua",
  "continúa",
  "continuar",
  "procede",
  "proceder",
  "acepto",
  "aceptar",
  "confirmo",
  "confirmar",
  "generalo",
  "genera",
  "adelante",
];

export function isAffirmative(text = "") {
  const normalized = text
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[.,!¡?¿]/g, "")
    .trim();

  return AFFIRMATIVE_KEYWORDS.some(
    (word) => normalized === word || normalized.includes(word)
  );
}
