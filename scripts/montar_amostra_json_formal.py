# Databricks notebook source
# =============================================================================
# montar_amostra_json_formal.py
#
# Constrói a amostra formal (Seção 5 do briefing oficial) -- pipeline
# PRÓPRIO, independente do report_agent (que é caixa-preta, só devolve
# HTML). Roda direto em cima dos .txt/.json que os dispatchers já salvam
# no Volume.
#
# REVISADO para conformidade com o schema oficial do briefing
# (briefing_desafiantes_kinea_research.pdf, Seção 5.2). Mudanças desta
# versão em relação à anterior:
#   - Campos novos: id (UUID v4), captured_at, language, area, subarea,
#     priority, source_name, source_url, source_type, summary,
#     sentiment_score, cluster_size, cluster_sources
#   - "area" fixo em "Infra" (decisão confirmada com o usuário -- o
#     schema oficial só previa CRI|CRA|SS|múltiplo; a granularidade de
#     setor que já usávamos (Energia e Gás, Saneamento, etc.) migrou para
#     "subarea", que é onde o schema oficial já guarda esse tipo de
#     detalhe fino (ex.: CRA usa subarea="sucroalcooleiro")
#   - sentiment: renomeado de sentiment_credor -> sentiment (nome exigido
#     pelo schema), valores em minúsculo; ganhou sentiment_score numérico
#     (-1.0 a +1.0) na MESMA chamada de LLM (evita chamada extra)
#   - relevance_score: reformulado de "só menção de portfólio, 4 degraus"
#     para fórmula contínua (0.0-1.0) combinando os 4 fatores sugeridos
#     na Seção 6.4 do briefing: prioridade da fonte, presença de
#     portfólio, valor monetário relevante, tags críticas -- TUDO
#     calculado por código, documentado, auditável (nunca "caixa preta")
#
# Etapas:
#   A. Seleção de amostra representativa (código, não LLM)
#   B. Clustering determinístico por similaridade de título (código, não
#      LLM -- cluster_id precisa ser reproduzível). NOTA: o briefing
#      oficial (Seção 7.1) recomenda clustering semântico via embeddings
#      -- este script ainda usa comparação de título por string, mais
#      simples; upgrade para embeddings é item separado, ainda pendente.
#   C. Enriquecimento por notícia (riscos de crédito, sentimento+score,
#      resumo, tags, relevance_score)
#   D. Montagem do JSON por notícia, no formato do schema oficial
#
# IMPORTANTE: `chamar_llm(system, user)` e `extrair_riscos_credito(...)`
# são fornecidas pelo ambiente via %run -- este script não as redefine.
# `spark` também é fornecido pelo ambiente Databricks.
# =============================================================================

# COMMAND ----------

import os
import re
import json
import uuid
import unicodedata
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

# COMMAND ----------

# =============================================================================
# Config
# =============================================================================

BASE_VOLUME_PATH = "/Volumes/desafio_kinea/research/research_volume/infraestrutura"
FILES_ROOT = os.path.join(BASE_VOLUME_PATH, "files")
SAIDA_ROOT = os.path.join(BASE_VOLUME_PATH, "outputs_nlp_formal")

CATALOGO = "desafio_kinea"
SCHEMA = "research"

TAMANHO_AMOSTRA = 120
JANELA_DIAS_AMOSTRA = 5
LIMIAR_CLUSTER_SIMILARIDADE = 0.55

AREA_FIXA = "Infra"  # decisao confirmada com o usuario -- schema oficial nao previa este valor

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

TAGS_CRITICAS = {"fato_relevante", "rating_change", "m_a", "default", "litigation"}

# COMMAND ----------

# =============================================================================
# Lookup de fontes -- source_name, subarea (setor), priority, source_type
#
# Puxa de controle_fontes (mesma tabela usada em todo o projeto). Como
# source_url e source_type nao existem como colunas proprias nessa
# tabela hoje, sao aproximados:
#   - source_url: dominio raiz extraido da propria URL do item
#   - source_type: heuristica a partir de tipo_categoria (ver mapa
#     abaixo) -- vale revisao manual depois, marcado como aproximacao
# =============================================================================

MAPA_TIPO_CATEGORIA_PARA_SOURCE_TYPE = {
    "Diário Oficial": "regulatorio",
    "Agência Reguladora": "regulatorio",
    "Mídia": "noticia",
    "Mídia especializada": "noticia",
    "Associação": "doc_corporativo",
    "Consultoria": "blog",
    "Agência de Rating": "rating",
}


def carregar_lookup_fontes():
    df_fontes = spark.table(f"{CATALOGO}.{SCHEMA}.controle_fontes").toPandas()
    lookup = {}
    for _, row in df_fontes.iterrows():
        source_id = row.get("source_id")
        if not source_id or source_id == "—":
            continue

        importancia = row.get("importancia_original")
        prioridade_int = {"Alta": 1, "Média": 2, "Media": 2, "Baixa": 3}.get(importancia, 3)

        tipo_categoria = row.get("tipo_categoria")
        source_type = MAPA_TIPO_CATEGORIA_PARA_SOURCE_TYPE.get(tipo_categoria, "noticia")

        lookup[source_id] = {
            "source_name": row.get("nome_fonte") or source_id,
            "subarea": row.get("setor") or "Não classificado",
            "priority": prioridade_int,
            "source_type": source_type,
        }
    return lookup


def extrair_dominio_raiz(url):
    import urllib.parse
    try:
        partes = urllib.parse.urlparse(url)
        return f"{partes.scheme}://{partes.netloc}" if partes.netloc else ""
    except Exception:
        return ""

# COMMAND ----------

# =============================================================================
# Etapa A -- Selecao de amostra representativa (codigo puro, sem LLM)
# =============================================================================

def _listar_documentos_do_dia(data):
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


def selecionar_amostra(tamanho_alvo, janela_dias):
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

    for data, docs in por_dia.items():
        cota = max(1, round(tamanho_alvo * len(docs) / total_disponivel))
        passo = max(1, len(docs) // cota)
        selecionados = docs[::passo][:cota]
        amostra.extend(selecionados)

    amostra = amostra[:tamanho_alvo]
    print(f"\nAmostra final: {len(amostra)} documentos (alvo era {tamanho_alvo})")
    return amostra

# COMMAND ----------

# =============================================================================
# Etapa B -- Clustering deterministico (codigo, sem LLM)
# =============================================================================

def _normalizar_titulo(titulo):
    titulo = unicodedata.normalize("NFKD", titulo or "").encode("ascii", "ignore").decode()
    titulo = re.sub(r"[^a-zA-Z0-9 ]", "", titulo).lower().strip()
    return titulo


def _slugify(texto, max_len=40):
    texto = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    texto = re.sub(r"[^a-zA-Z0-9]+", "-", texto).strip("-").lower()
    return (texto[:max_len] or "sem-titulo").strip("-")


def atribuir_clusters(amostra):
    por_data = {}
    for doc in amostra:
        por_data.setdefault(doc["data"], []).append(doc)

    for data, docs in por_data.items():
        clusters = []
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


def calcular_cluster_size_e_sources(amostra):
    info = {}
    for doc in amostra:
        cid = doc["cluster_id"]
        sid = doc["metadados"].get("source_id", "")
        if cid not in info:
            info[cid] = {"size": 0, "sources": set()}
        info[cid]["size"] += 1
        if sid:
            info[cid]["sources"].add(sid)
    return {cid: {"size": v["size"], "sources": sorted(v["sources"])} for cid, v in info.items()}

# COMMAND ----------

# =============================================================================
# Etapa C.1 -- relevance_score (codigo puro, formula documentada)
#
# Formula combinando os 4 fatores sugeridos na Secao 6.4 do briefing
# oficial: (a) prioridade da fonte, (b) presenca de portfolio, (c) valor
# monetario relevante, (d) tags criticas. Pesos somam 1.0; cada fator
# documentado no rationale, nada de caixa-preta.
# =============================================================================

THRESHOLD_VALOR_RELEVANTE = 100_000_000  # R$ 100 milhoes

PADRAO_VALOR = re.compile(
    r'R\$\s*([\d.,]+)\s*(milh[ãa]o|milh[õo]es|bilh[ãa]o|bilh[õo]es|\bmi\b|\bbi\b|\bmil\b)?',
    re.IGNORECASE
)
MULTIPLICADORES_VALOR = {
    "mil": 1_000,
    "mi": 1_000_000, "milhão": 1_000_000, "milhao": 1_000_000, "milhões": 1_000_000, "milhoes": 1_000_000,
    "bi": 1_000_000_000, "bilhão": 1_000_000_000, "bilhao": 1_000_000_000, "bilhões": 1_000_000_000, "bilhoes": 1_000_000_000,
}


def contem_valor_monetario_relevante(texto, threshold=THRESHOLD_VALOR_RELEVANTE):
    for match in PADRAO_VALOR.finditer(texto or ""):
        numero_str, unidade = match.groups()
        numero_str = numero_str.replace(".", "").replace(",", ".")
        try:
            numero = float(numero_str)
        except ValueError:
            continue
        multiplicador = MULTIPLICADORES_VALOR.get((unidade or "").lower(), 1)
        if numero * multiplicador >= threshold:
            return True
    return False


def calcular_relevance_score(prioridade_fonte, riscos_mencionados, texto, tags):
    peso_prioridade = {1: 1.0, 2: 0.6, 3: 0.3}.get(prioridade_fonte, 0.3)

    n_portfolio = len([r for r in riscos_mencionados if r.get("in_kinea_universe")])
    if n_portfolio == 0: peso_portfolio = 0.0
    elif n_portfolio == 1: peso_portfolio = 0.5
    elif n_portfolio <= 3: peso_portfolio = 0.75
    else: peso_portfolio = 1.0

    tem_valor = contem_valor_monetario_relevante(texto)
    peso_valor = 1.0 if tem_valor else 0.0

    tags_criticas_presentes = TAGS_CRITICAS.intersection(tags or [])
    peso_tags = 1.0 if tags_criticas_presentes else 0.0

    score = 0.30 * peso_prioridade + 0.35 * peso_portfolio + 0.15 * peso_valor + 0.20 * peso_tags
    score = round(min(1.0, score), 3)

    rationale = (
        f"prioridade_fonte={prioridade_fonte}(peso {peso_prioridade}) x0.30 + "
        f"portfolio={n_portfolio} empresa(s)(peso {peso_portfolio}) x0.35 + "
        f"valor_monetario_relevante={tem_valor}(peso {peso_valor}) x0.15 + "
        f"tags_criticas={sorted(tags_criticas_presentes)}(peso {peso_tags}) x0.20 = {score}"
    )
    return score, rationale

# COMMAND ----------

# =============================================================================
# Etapa C.2 -- Sentimento do credor + score numerico (LLM, uma chamada so)
# =============================================================================

SYSTEM_SENTIMENTO_CREDOR = """You are a credit analyst assessing news from the perspective of a BONDHOLDER/CREDITOR of infrastructure concessionaires -- not a general market sentiment reader.

Given the article text, classify the sentiment STRICTLY from a creditor's viewpoint: does this news make the issuer's ability to service its debt look better, worse, or unchanged?

Return a JSON with exactly three fields:
- "sentiment": one of "positivo", "neutro", "negativo" (lowercase, exact strings)
- "sentiment_score": a float from -1.0 (very negative for creditors) to +1.0 (very positive for creditors)
- "justificativa": one sentence (max 25 words), in Portuguese (Brazil), explaining the credit-specific reasoning (not generic market reasoning)

Guidance:
- "positivo": strengthens cash flow predictability, reduces leverage, improves covenant headroom, positive rating action, successful refinancing at better terms.
- "negativo": weakens cash flow, increases leverage or refinancing risk, negative rating action, litigation with financial exposure, regulatory penalty with cash impact, cost overrun.
- "neutro": no discernible credit impact, or impact too indirect/speculative to classify either way.
- sentiment_score must be consistent with the category (positivo -> score > 0, negativo -> score < 0, neutro -> score close to 0).

Do not conflate "good news for equity" with "good news for credit" -- a large capex announcement can be negative for a creditor (leverage increase) while being neutral or positive for an equity holder. Stay strictly in the creditor's frame.

Article text:
{article_text}
"""


def avaliar_sentimento_credor(texto_artigo, chamar_llm):
    resposta = chamar_llm(
        system=SYSTEM_SENTIMENTO_CREDOR.format(article_text=texto_artigo[:4000]),
        user="Classifique o sentimento e o score numérico para este artigo.",
    )
    sentimento = resposta.get("sentiment")
    if sentimento not in ("positivo", "neutro", "negativo"):
        sentimento = "neutro"

    try:
        score = float(resposta.get("sentiment_score", 0.0))
        score = max(-1.0, min(1.0, score))
    except (TypeError, ValueError):
        score = 0.0

    return {
        "sentiment": sentimento,
        "sentiment_score": round(score, 3),
        "sentiment_justificativa": resposta.get("justificativa", ""),
    }

# COMMAND ----------

# =============================================================================
# Etapa C.3 -- Resumo (LLM) -- campo obrigatorio novo, Secao 6.1 do briefing
# =============================================================================

SYSTEM_SUMARIZACAO = """You are a financial news summarizer for a credit research desk.

Summarize the article in 2-3 sentences (~300-500 characters) in Portuguese (Brazil), even if the source article is in English.

Preserve: named entities (companies, people, agencies), monetary values, and critical dates -- do not drop these to save space.

Return a JSON with exactly one field:
- "summary": the 2-3 sentence summary in Portuguese

Article text:
{article_text}
"""


def gerar_resumo(texto_artigo, chamar_llm):
    resposta = chamar_llm(
        system=SYSTEM_SUMARIZACAO.format(article_text=texto_artigo[:4000]),
        user="Resuma este artigo.",
    )
    return resposta.get("summary", "")

# COMMAND ----------

# =============================================================================
# Etapa C.4 -- Tags (LLM, vocabulario fechado, multi-label)
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


def classificar_tags(texto_artigo, chamar_llm):
    resposta = chamar_llm(
        system=SYSTEM_TAGS_CONTROLADAS.format(article_text=texto_artigo[:4000]),
        user="Classifique as tags para este artigo.",
    )
    tags_brutas = resposta.get("tags", [])
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
# Etapa D -- Montagem do JSON por noticia (schema oficial, Secao 5.2)
# =============================================================================

def montar_objeto_noticia(doc, lookup_fontes, cluster_info, chamar_llm):
    metadados = doc["metadados"]
    texto = doc["texto"]
    source_id = metadados.get("source_id", "")

    info_fonte = lookup_fontes.get(source_id, {
        "source_name": source_id or "Desconhecida",
        "subarea": "Não classificado",
        "priority": 3,
        "source_type": "noticia",
    })

    # C.1 -- riscos de credito (reaproveita o modulo ja pronto)
    riscos = extrair_riscos_credito(texto, doc["data"], chamar_llm)

    # C.4 primeiro (tags), porque relevance_score usa as tags como um dos fatores
    tags_resultado = classificar_tags(texto, chamar_llm)

    # C.1b -- relevance_score (codigo puro, usa riscos + tags + prioridade)
    relevance_score, relevance_rationale = calcular_relevance_score(
        info_fonte["priority"], riscos, texto, tags_resultado["tags"]
    )

    # C.2 -- sentimento + score numerico
    sentimento = avaliar_sentimento_credor(texto, chamar_llm)

    # C.3 -- resumo
    resumo = gerar_resumo(texto, chamar_llm)

    cluster_id = doc.get("cluster_id", "")
    info_cluster = cluster_info.get(cluster_id, {"size": 1, "sources": [source_id] if source_id else []})

    url_item = metadados.get("url", "")

    return {
        "id": str(uuid.uuid4()),
        "cluster_id": cluster_id,
        "source_id": source_id,
        "source_name": info_fonte["source_name"],
        "source_url": extrair_dominio_raiz(url_item),
        "source_type": info_fonte["source_type"],
        "title": metadados.get("title", ""),
        "url": url_item,
        "published_at": metadados.get("published_at") or metadados.get("date") or "",
        "captured_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "language": "pt",
        "area": AREA_FIXA,
        "subarea": info_fonte["subarea"],
        "priority": info_fonte["priority"],
        "credit_risks_mentioned": riscos,
        "summary": resumo,
        "sentiment": sentimento["sentiment"],
        "sentiment_score": sentimento["sentiment_score"],
        "relevance_score": relevance_score,
        "relevance_score_rationale": relevance_rationale,
        "tags": tags_resultado["tags"],
        "tag_sugerida_fora_do_vocabulario": tags_resultado["tag_sugerida_fora_do_vocabulario"],
        "cluster_size": info_cluster["size"],
        "cluster_sources": info_cluster["sources"],
    }

# COMMAND ----------

# =============================================================================
# Execucao
# =============================================================================

if __name__ == "__main__":
    os.makedirs(SAIDA_ROOT, exist_ok=True)

    print("=== Carregando lookup de fontes (controle_fontes) ===")
    lookup_fontes = carregar_lookup_fontes()
    print(f"  {len(lookup_fontes)} fontes no lookup")

    print("\n=== Etapa A: selecionando amostra ===")
    amostra = selecionar_amostra(TAMANHO_AMOSTRA, JANELA_DIAS_AMOSTRA)

    print("\n=== Etapa B: atribuindo clusters (determinístico) ===")
    amostra = atribuir_clusters(amostra)
    cluster_info = calcular_cluster_size_e_sources(amostra)
    print(f"  {len(amostra)} notícias agrupadas em {len(cluster_info)} clusters")

    print("\n=== Etapa C+D: enriquecendo e montando o JSON por notícia ===")
    resultado = []
    for i, doc in enumerate(amostra, start=1):
        titulo_curto = (doc["metadados"].get("title") or "")[:60]
        print(f"  [{i}/{len(amostra)}] {titulo_curto}")
        try:
            objeto = montar_objeto_noticia(doc, lookup_fontes, cluster_info, chamar_llm)
            resultado.append(objeto)
        except Exception as e:
            print(f"    [ERRO] pulando esta notícia: {e}")

    caminho_saida = os.path.join(SAIDA_ROOT, f"amostra_formal_{datetime.today().strftime('%Y-%m-%d')}.json")
    with open(caminho_saida, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"\n=== Concluído: {len(resultado)} notícias salvas em {caminho_saida} ===")
