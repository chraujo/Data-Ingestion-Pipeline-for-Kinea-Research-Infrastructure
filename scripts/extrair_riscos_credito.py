# Databricks notebook source
# =============================================================================
# extrair_riscos_credito.py
#
# Estágio de enriquecimento NLP: extração de riscos de crédito mencionados
# numa notícia, conforme Seção 6.2 do briefing formal do desafio.
#
# Pipeline: LLM extrai nomes de empresas/grupos citados no texto (sem tentar
# adivinhar o canonical_id sozinho) -> fuzzy match determinístico contra o
# dicionário canônico da Kinea decide o canonical_id (ou null).
#
# Por que dividir em duas etapas (LLM extrai, código faz o match) em vez de
# pedir pro LLM já devolver o canonical_id direto: o match é a parte que
# precisa ser auditável e determinística (o briefing pede isso
# explicitamente pro relevance_score, e o mesmo princípio vale aqui) — um
# fuzzy match com threshold fixo é conferível e reproduzível; deixar o LLM
# "decidir" o ID sozinho seria uma caixa-preta sujeita a alucinação de
# canonical_id inexistente.
#
# REVISADO EM 05/08/2026 — as 4 ambiguidades identificadas na primeira
# versão foram resolvidas com confirmação do usuário:
#   - TAESA, Vibra Energia e Comerc: separadas em 3 canonical_id distintos
#     (estavam erroneamente juntas).
#   - Grupo Cosan (Cosan, Rumo, Comgás, Raízen, Moove, etc.): mantidas
#     juntas de propósito, como um risco de crédito consolidado
#     ("rumo_malha_paulista_s_a").
#   - CCR e Copacabana Energia: separadas em 2 canonical_id distintos.
#   - BRK Ambiental / Concessionário Via Rio: ticker CTOL18 duplicado
#     corrigido — CTOL18/CTOL28 agora pertencem só à Via Rio; BRK Ambiental
#     Mauá ficou só com sua própria SPE, e as referências genéricas ("BRK",
#     "BRKPA1") foram para o grupo da holding (BRK Ambiental Participações).
#
# A partir de 05/08/2026, a lista canônica NÃO fica mais embutida neste
# arquivo — vive em scripts/canonical_entidades.json, compartilhada com o
# gera_config.ipynb (que a usa pra citar empresas de carteira no briefing
# diário). Editar sempre o JSON, nunca duplicar a lista aqui.
# =============================================================================

# COMMAND ----------

import json as _json

_CAMINHO_CANONICAL = (
    "/Workspace/Shared/Research_Infra/"
    "Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure/"
    "scripts/canonical_entidades.json"
)

with open(_CAMINHO_CANONICAL, encoding="utf-8") as _f:
    CANONICAL_ENTIDADES = _json.load(_f)

# COMMAND ----------

# =============================================================================
# Etapa 1 — LLM extrai nomes de entidades mencionadas (sem tentar casar
# contra a lista canônica sozinho)
# =============================================================================

SYSTEM_EXTRAIR_ENTIDADES_PROMPT = """You are an entity-extraction analyst for the Infrastructure investment team of a Brazilian asset manager (Kinea). Today is {today}.

You are given the text of one news article. Your task: extract every company, economic group, or corporate entity mentioned in the article that could plausibly be a credit risk (issuer, concessionaire, SPE, holding, or their direct parent group) — not generic sector terms, not regulators, not government bodies.

## What counts as an entity to extract
- Named companies, concessionaires, SPEs, holdings (e.g. "Ecorodovias", "TAESA", "Usina Coruripe").
- Economic groups referenced by their common name (e.g. "Grupo Cosan", "Aegea").
- Tickers if mentioned inline (e.g. "TAEE11").

## What NOT to extract
- Regulators and government bodies (ANEEL, ANTT, ANP, CVM, BCB, ministries).
- Generic sector references ("o setor elétrico", "as concessionárias de rodovias" without a specific name).
- Geographic locations, indices, or macro indicators.

## Output
Return a JSON with an `entities_mentioned` array. Each element:
- `name` (string): the entity name exactly as it appears in the text (preserve original casing/spelling — do NOT normalize or correct it).
- `context` (string): the short clause or sentence fragment where it appears (max 15 words), for audit purposes.

If no entities are found, return `{{"entities_mentioned": []}}`.
"""

USER_EXTRAIR_ENTIDADES_PROMPT = """Article text:

{article_text}"""

# COMMAND ----------

# =============================================================================
# Etapa 2 — fuzzy match determinístico contra o dicionário canônico
# =============================================================================

from rapidfuzz import fuzz, process

LIMIAR_MATCH = 0.85  # mesmo threshold sugerido pelo briefing (Seção 6.2)


def _normalizar(texto: str) -> str:
    import unicodedata
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return texto.strip().upper()


def _construir_indice_aliases():
    """Mapa alias_normalizado -> canonical_id, para lookup exato O(1) antes do fuzzy."""
    indice = {}
    for entidade in CANONICAL_ENTIDADES:
        for alias in entidade["aliases"]:
            indice[_normalizar(alias)] = entidade["canonical_id"]
    return indice


_INDICE_ALIASES = _construir_indice_aliases()
_TODOS_OS_ALIASES = list(_INDICE_ALIASES.keys())


def casar_entidade(nome_extraido: str) -> dict:
    """
    Recebe um nome de entidade como o LLM extraiu (texto livre) e devolve
    o objeto no formato exigido pelo briefing (Seção 5.2):
    {"name": ..., "canonical_id": ... | None, "in_kinea_universe": bool}
    """
    nome_normalizado = _normalizar(nome_extraido)

    # 1. Match exato primeiro (mais confiável, e mais rápido)
    if nome_normalizado in _INDICE_ALIASES:
        return {
            "name": nome_extraido,
            "canonical_id": _INDICE_ALIASES[nome_normalizado],
            "in_kinea_universe": True,
        }

    # 2. Fuzzy match (cobre variações de grafia, abreviação, etc.)
    resultado = process.extractOne(
        nome_normalizado, _TODOS_OS_ALIASES, scorer=fuzz.token_set_ratio
    )
    if resultado is not None:
        alias_encontrado, score, _ = resultado
        if score / 100 >= LIMIAR_MATCH:
            return {
                "name": nome_extraido,
                "canonical_id": _INDICE_ALIASES[alias_encontrado],
                "in_kinea_universe": True,
            }

    # 3. Sem match — ainda registra a menção, mas fora do universo Kinea
    return {"name": nome_extraido, "canonical_id": None, "in_kinea_universe": False}

# COMMAND ----------

# =============================================================================
# Pipeline completo: chama o LLM (via API já usada no gera_config.ipynb) e
# aplica o match em cada entidade extraída
# =============================================================================

def extrair_riscos_credito(article_text: str, today: str, chamar_llm) -> list[dict]:
    """
    `chamar_llm` é a função/cliente já usado no resto do pipeline (mesma
    integração usada pelos outros prompts do gera_config.ipynb) — não
    reimplementamos chamada de API aqui, só reaproveitamos.
    """
    resposta = chamar_llm(
        system=SYSTEM_EXTRAIR_ENTIDADES_PROMPT.format(today=today),
        user=USER_EXTRAIR_ENTIDADES_PROMPT.format(article_text=article_text),
    )
    entidades_brutas = resposta.get("entities_mentioned", [])
    return [casar_entidade(e["name"]) for e in entidades_brutas]

# COMMAND ----------

# =============================================================================
# Teste rápido com dado fictício (rodar isolado, sem LLM, só pra validar o
# matching em si)
# =============================================================================

if __name__ == "__main__":
    testes = ["Taesa", "TAEE11", "Ecorodovias Concessões", "Empresa Desconhecida XYZ",
              "Aegea Saneamento", "aguas do rio 1"]
    for nome in testes:
        print(casar_entidade(nome))
