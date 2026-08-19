# Databricks notebook source
# =============================================================================
# montar_amostra_json_formal.py
#
# Constrói a amostra formal (Seção 5 do briefing) -- pipeline PRÓPRIO,
# independente do report_agent (que é caixa-preta, só devolve HTML). Roda
# direto em cima dos .txt/.json que os dispatchers já salvam no Volume.
#
# Etapas:
#   A. Seleção de amostra representativa (código, não LLM)
#   B. Clustering determinístico (código, não LLM -- cluster_id precisa
#      ser reproduzível, diferente do clustering "criativo" do LLM usado
#      no briefing diário)
#   C. Enriquecimento por notícia:
#      - riscos de crédito -> reaproveita extrair_riscos_credito.py, sem
#        reescrever a lógica
#      - relevance_score -> calculado por CÓDIGO, fórmula documentada
#        abaixo (por menção de portfólio, decisão confirmada com o
#        usuário -- não é o LLM "achando" um número)
#      - sentimento do credor -> LLM, Positivo/Neutro/Negativo
#      - tags -> LLM, vocabulário fechado (confirmado com o usuário),
#        multi-label
#   D. Montagem do JSON por notícia, no formato do schema formal
#
# IMPORTANTE: `chamar_llm(system, user)` é fornecida pelo ambiente
# (mesma função já usada em extrair_riscos_credito.py) -- este script não
# reimplementa chamada de API.
# =============================================================================

# COMMAND ----------

import os
import re
import json
import unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher

# COMMAND ----------

# =============================================================================
# Config
# =============================================================================

BASE_VOLUME_PATH = "/Volumes/desafio_kinea/research/research_volume/infraestrutura"
FILES_ROOT = os.path.join(BASE_VOLUME_PATH, "files")
SAIDA_ROOT = os.path.join(BASE_VOLUME_PATH, "outputs_nlp_formal")

TAMANHO_AMOSTRA = 120          # alvo -- acima do minimo de 100 exigido, com folga
JANELA_DIAS_AMOSTRA = 5        # de quantos dias recentes puxar a amostra
LIMIAR_CLUSTER_SIMILARIDADE = 0.55   # titulos acima disso (SequenceMatcher) viram o mesmo cluster

VOCABULARIO_TAGS = [
    "fato_relevante", "comunicado_cvm", "resultado_trimestral",
    "emissao_debenture", "emissao_cri", "emissao_cra", "captacao",
    "m_a", "ipo", "follow_on",
    "rating_change",
    "default", "renegociacao", "recuperacao_judicial",
    "litigation", "regulatorio", "governanca",
    "guidance", "projeto_novo", "expansao", "desinvestimento",
    "esg", "safra", "lancamento",
]

# COMMAND ----------

# =============================================================================
# Etapa A -- Seleção de amostra representativa (código puro, sem LLM)
#
# Estratégia: pega os N dias mais recentes, distribui a amostra
# proporcionalmente entre eles (não só o dia mais recente, para ter
# variedade temporal), e dentro de cada dia distribui entre os setores
# disponíveis (via subpasta, se existir -- ou trata tudo como um grupo só
# se a estrutura for plana).
# =============================================================================

def _listar_documentos_do_dia(data: str) -> list[dict]:
    pasta_dia = os.path.join(FILES_ROOT, data)
    if not os.path.exists(pasta_dia):
        return []

    documentos = []
    for raiz, _dirs, arquivos in os.walk(pasta_dia):
        for nome_arquivo in arquivos:
            if not nome_arquivo.endswith(".json"):
                continue
            caminho_json = os.path.join(raiz, nome_arquivo)
            caminho_txt = caminho_json[: -len(".json")] + ".txt"
            if not os.path.exists(caminho_txt):
                continue
            try:
                metadados = json.load(open(caminho_json, encoding="utf-8"))
            except Exception:
                continue
            with open(caminho_txt, encoding="utf-8") as f:
                texto = f.read()
            if not texto.strip():
                continue
            documentos.append({
                "metadados": metadados,
                "texto": texto,
                "data": data,
            })
    return documentos


def selecionar_amostra(tamanho_alvo: int, janela_dias: int) -> list[dict]:
    hoje = datetime.today()
    datas = [(hoje - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(janela_dias)]

    por_dia = {}
    for data in datas:
        docs = _listar_documentos_do_dia(data)
        if docs:
            por_dia[data] = docs
            print(f"  {data}: {len(docs)} documentos disponíveis")

    if not por_dia:
        raise ValueError(f"Nenhum documento encontrado nos últimos {janela_dias} dias.")

    total_disponivel = sum(len(v) for v in por_dia.values())
    amostra = []

    # Distribui proporcionalmente entre os dias que têm documento
    for data, docs in por_dia.items():
        cota = max(1, round(tamanho_alvo * len(docs) / total_disponivel))
        # Amostragem determinística: pega espaçado (não aleatório), pra
        # reprodutibilidade -- roda de novo, mesmo resultado.
        passo = max(1, len(docs) // cota)
        selecionados = docs[::passo][:cota]
        amostra.extend(selecionados)

    amostra = amostra[:tamanho_alvo]
    print(f"\nAmostra final: {len(amostra)} documentos (alvo era {tamanho_alvo})")
    return amostra

# COMMAND ----------

# =============================================================================
# Etapa B -- Clustering determinístico (código, sem LLM)
#
# cluster_id no formato cl_YYYY-MM-DD_slug_NN, exigido pelo schema formal.
# Agrupa por: mesma data + título com similaridade acima do limiar. É
# simples de propósito -- prioriza ser reproduzível e auditável sobre ser
# "esperto" (esse é o trabalho do LLM no briefing diário, não aqui).
# =============================================================================

def _normalizar_titulo(titulo: str) -> str:
    titulo = unicodedata.normalize("NFKD", titulo or "").encode("ascii", "ignore").decode()
    titulo = re.sub(r"[^a-zA-Z0-9 ]", "", titulo).lower().strip()
    return titulo


def _slugify(texto: str, max_len: int = 40) -> str:
    texto = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    texto = re.sub(r"[^a-zA-Z0-9]+", "-", texto).strip("-").lower()
    return (texto[:max_len] or "sem-titulo").strip("-")


def atribuir_clusters(amostra: list[dict]) -> list[dict]:
    por_data = {}
    for doc in amostra:
        por_data.setdefault(doc["data"], []).append(doc)

    for data, docs in por_data.items():
        clusters = []  # lista de {"titulo_normalizado": str, "membros": [doc, ...]}
        for doc in docs:
            titulo = doc["metadados"].get("title", "")
            titulo_norm = _normalizar_titulo(titulo)

            cluster_encontrado = None
            for cluster in clusters:
                similaridade = SequenceMatcher(None, titulo_norm, cluster["titulo_normalizado"]).ratio()
                if similaridade >= LIMIAR_CLUSTER_SIMILARIDADE:
                    cluster_encontrado = cluster
                    break

            if cluster_encontrado:
                cluster_encontrado["membros"].append(doc)
            else:
                clusters.append({"titulo_normalizado": titulo_norm, "titulo_original": titulo, "membros": [doc]})

        for i, cluster in enumerate(clusters, start=1):
            slug = _slugify(cluster["titulo_original"])
            cluster_id = f"cl_{data}_{slug}_{i:02d}"
            for doc in cluster["membros"]:
                doc["cluster_id"] = cluster_id

    return amostra

# COMMAND ----------

# =============================================================================
# Etapa C.1 -- relevance_score (código puro, fórmula documentada)
#
# FÓRMULA (decisão confirmada com o usuário: por menção de portfólio):
#   0.0  -> nenhuma empresa do portfólio mencionada
#   0.5  -> 1 empresa mencionada
#   0.7  -> 2-3 empresas mencionadas
#   1.0  -> 4+ empresas mencionadas, OU a notícia é exclusivamente sobre
#           1 empresa do portfólio (nome dela aparece no título)
#
# Reaproveita DIRETO a saída de extrair_riscos_credito() -- não faz
# nenhuma chamada de LLM própria para esse número.
# =============================================================================

def calcular_relevance_score(riscos_mencionados: list[dict], titulo: str) -> tuple[float, str]:
    empresas_no_universo = [r for r in riscos_mencionados if r.get("in_kinea_universe")]
    n = len(empresas_no_universo)

    titulo_norm = _normalizar_titulo(titulo)
    foco_exclusivo = n == 1 and _normalizar_titulo(empresas_no_universo[0]["name"]) in titulo_norm

    if n == 0:
        return 0.0, "Nenhuma empresa do portfólio Kinea mencionada."
    if foco_exclusivo:
        return 1.0, f"Notícia focada exclusivamente em {empresas_no_universo[0]['name']} (portfólio Kinea)."
    if n == 1:
        return 0.5, f"1 empresa do portfólio mencionada: {empresas_no_universo[0]['name']}."
    if n <= 3:
        nomes = ", ".join(e["name"] for e in empresas_no_universo)
        return 0.7, f"{n} empresas do portfólio mencionadas: {nomes}."
    nomes = ", ".join(e["name"] for e in empresas_no_universo[:4]) + ("..." if n > 4 else "")
    return 1.0, f"{n} empresas do portfólio mencionadas: {nomes}."

# COMMAND ----------

# =============================================================================
# Etapa C.2 -- Sentimento do credor (LLM)
# =============================================================================

SYSTEM_SENTIMENTO_CREDOR = """You are a credit analyst assessing news from the perspective of a BONDHOLDER/CREDITOR of infrastructure concessionaires -- not a general market sentiment reader.

Given the article text, classify the sentiment STRICTLY from a creditor's viewpoint: does this news make the issuer's ability to service its debt look better, worse, or unchanged?

Return a JSON with exactly two fields:
- "sentimento_credor": one of "Positivo", "Neutro", "Negativo"
- "justificativa": one sentence (max 25 words), in Portuguese (Brazil), explaining the credit-specific reasoning (not generic market reasoning).

Guidance:
- "Positivo": strengthens cash flow predictability, reduces leverage, improves covenant headroom, positive rating action, successful refinancing at better terms.
- "Negativo": weakens cash flow, increases leverage or refinancing risk, negative rating action, litigation with financial exposure, regulatory penalty with cash impact, cost overrun.
- "Neutro": no discernible credit impact, or impact too indirect/speculative to classify either way, or the news isn't about a credit-relevant event at all.

Do not conflate "good news for equity" with "good news for credit" -- a large capex announcement can be negative for a creditor (leverage increase) while being neutral or positive for an equity holder. Stay strictly in the creditor's frame.

Article text:
{article_text}
"""


def avaliar_sentimento_credor(texto_artigo: str, chamar_llm) -> dict:
    resposta = chamar_llm(
        system=SYSTEM_SENTIMENTO_CREDOR.format(article_text=texto_artigo[:4000]),
        user="Classifique o sentimento do credor para este artigo.",
    )
    sentimento = resposta.get("sentimento_credor")
    if sentimento not in ("Positivo", "Neutro", "Negativo"):
        sentimento = "Neutro"  # fallback seguro se o LLM devolver algo fora do vocabulário
    return {
        "sentimento_credor": sentimento,
        "sentimento_justificativa": resposta.get("justificativa", ""),
    }

# COMMAND ----------

# =============================================================================
# Etapa C.3 -- Tags (LLM, vocabulário fechado, multi-label)
# =============================================================================

SYSTEM_TAGS_CONTROLADAS = """You are a tagging analyst for a credit research desk. Classify the article using ONLY tags from the CLOSED vocabulary below -- never invent a new tag name in the main "tags" field.

Closed vocabulary (use the exact strings, lowercase, with underscores):
fato_relevante, comunicado_cvm, resultado_trimestral,
emissao_debenture, emissao_cri, emissao_cra, captacao,
m_a, ipo, follow_on,
rating_change,
default, renegociacao, recuperacao_judicial,
litigation, regulatorio, governanca,
guidance, projeto_novo, expansao, desinvestimento,
esg, safra, lancamento

Return a JSON with:
- "tags" (array of strings): 1 to 4 tags from the vocabulary above that genuinely apply. Never pad with a weak match just to fill space -- prefer fewer, correct tags.
- "tag_sugerida_fora_do_vocabulario" (string or null): if you believe a tag NOT in the vocabulary would describe this article better or in addition, name it here (snake_case). This is a suggestion for human review, NOT an official tag -- leave null if the existing vocabulary already covers it well.

Article text:
{article_text}
"""


def classificar_tags(texto_artigo: str, chamar_llm) -> dict:
    resposta = chamar_llm(
        system=SYSTEM_TAGS_CONTROLADAS.format(article_text=texto_artigo[:4000]),
        user="Classifique as tags para este artigo.",
    )
    tags_brutas = resposta.get("tags", [])
    # Defesa: descarta silenciosamente qualquer tag que o LLM tenha
    # inventado fora do vocabulário, em vez de deixar vazar pro JSON
    # final -- o vocabulário fechado é uma garantia de código, não só de
    # prompt.
    tags_validas = [t for t in tags_brutas if t in VOCABULARIO_TAGS]
    tags_invalidas = [t for t in tags_brutas if t not in VOCABULARIO_TAGS]
    if tags_invalidas:
        print(f"    [aviso] tag(s) fora do vocabulário descartada(s): {tags_invalidas}")

    return {
        "tags": tags_validas,
        "tag_sugerida_fora_do_vocabulario": resposta.get("tag_sugerida_fora_do_vocabulario"),
    }

# COMMAND ----------

# =============================================================================
# Etapa D -- Montagem do JSON por notícia (schema formal, Seção 5)
# =============================================================================

def montar_objeto_noticia(doc: dict, chamar_llm) -> dict:
    metadados = doc["metadados"]
    texto = doc["texto"]

    # C.1 -- riscos de crédito (reaproveita o módulo já pronto)
    riscos = extrair_riscos_credito(texto, doc["data"], chamar_llm)

    # C.1b -- relevance_score (código puro, usa a saída acima)
    relevance_score, relevance_rationale = calcular_relevance_score(riscos, metadados.get("title", ""))

    # C.2 -- sentimento do credor
    sentimento = avaliar_sentimento_credor(texto, chamar_llm)

    # C.3 -- tags
    tags_resultado = classificar_tags(texto, chamar_llm)

    return {
        "source_id": metadados.get("source_id", ""),
        "title": metadados.get("title", ""),
        "url": metadados.get("url", ""),
        "published_at": metadados.get("published_at") or metadados.get("date") or "",
        "date_processed": doc["data"],
        "cluster_id": doc.get("cluster_id", ""),
        "credit_risks_mentioned": riscos,
        "relevance_score": relevance_score,
        "relevance_score_rationale": relevance_rationale,
        "sentiment_credor": sentimento["sentimento_credor"],
        "sentiment_justificativa": sentimento["sentimento_justificativa"],
        "tags": tags_resultado["tags"],
        "tag_sugerida_fora_do_vocabulario": tags_resultado["tag_sugerida_fora_do_vocabulario"],
    }

# COMMAND ----------

# =============================================================================
# Execução
# =============================================================================
#
# `chamar_llm` e `extrair_riscos_credito` precisam estar disponíveis no
# ambiente antes de rodar esta célula -- via %run do
# scripts/extrair_riscos_credito.py, e a função de chamada de LLM já usada
# em produção.

if __name__ == "__main__":
    os.makedirs(SAIDA_ROOT, exist_ok=True)

    print("=== Etapa A: selecionando amostra ===")
    amostra = selecionar_amostra(TAMANHO_AMOSTRA, JANELA_DIAS_AMOSTRA)

    print("\n=== Etapa B: atribuindo clusters (determinístico) ===")
    amostra = atribuir_clusters(amostra)
    n_clusters = len(set(d["cluster_id"] for d in amostra))
    print(f"  {len(amostra)} notícias agrupadas em {n_clusters} clusters")

    print("\n=== Etapa C+D: enriquecendo e montando o JSON por notícia ===")
    resultado = []
    for i, doc in enumerate(amostra, start=1):
        titulo_curto = (doc["metadados"].get("title") or "")[:60]
        print(f"  [{i}/{len(amostra)}] {titulo_curto}")
        try:
            objeto = montar_objeto_noticia(doc, chamar_llm)
            resultado.append(objeto)
        except Exception as e:
            print(f"    [ERRO] pulando esta notícia: {e}")

    caminho_saida = os.path.join(SAIDA_ROOT, f"amostra_formal_{datetime.today().strftime('%Y-%m-%d')}.json")
    with open(caminho_saida, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"\n=== Concluído: {len(resultado)} notícias salvas em {caminho_saida} ===")
