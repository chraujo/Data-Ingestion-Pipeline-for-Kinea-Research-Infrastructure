# Databricks notebook source
# =============================================================================
# registrar_fonte_tcu_solucao_consensual.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "TCU - Notícias (Solução Consensual)". Já existia linha placeholder
# "SECEX CONSENSO" em controle_fontes (catálogo original, nunca
# implementada -- source_id="—"). UPDATE na linha existente em vez de
# INSERT, mesmo padrão de ONS/ANP/ABEGAS/ANATEL/Trata Brasil/CCEE/ABRACE/
# AESBE -- NÃO cria fonte nova com outro nome (nome do catálogo é
# "SECEX CONSENSO", não "TCU").
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Órgãos Públicos"
#   setor = "Geral" -- consistente com o caráter transversal descrito no
#   pedido (Regulatório/Múltiplo -- energia, transporte, telecom,
#   óleo&gás), sem necessidade de reclassificar.
#   importancia_original = "Média" -- já vinha definida no catálogo
#   original, sem necessidade de reconferir.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "tcu_solucao_consensual"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'SECEX CONSENSO'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'SECEX CONSENSO' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'SECEX CONSENSO' encontrado, nenhum source_id duplicado; prosseguindo.")

# COMMAND ----------

linha_notebook = spark.sql(f"""
    SELECT notebook_nome, fontes_cobertas FROM {TABELA_NOTEBOOKS}
    WHERE notebook_nome = '{NOTEBOOK_NOME}'
""").collect()

if not linha_notebook:
    raise RuntimeError(f"notebook_nome='{NOTEBOOK_NOME}' não encontrado em {TABELA_NOTEBOOKS}; esperado já existir.")

fontes_atuais = linha_notebook[0]["fontes_cobertas"] or ""
print(f"[info] fontes_cobertas atual de '{NOTEBOOK_NOME}': {fontes_atuais!r}")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_fontes
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        source_id = '{SOURCE_ID}',
        status = 'Não iniciada',
        metodo_captura = 'Scraping HTML renderizado no servidor (Next.js App Router, SSR/RSC) via dispatcher generico ingest-scraping, com curl_cffi (impersonation de TLS) para contornar bloqueio de bot Akamai, paginacao por query string (&pagina=N)',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'Listagem filtrada por tema (?tema=Solucao+consensual) -- ja vem pronta no HTML (SSR de verdade, diferente da ANATEL onde precisou de API). Bloqueio de bot na requisicao simples (challenge JS Akamai, cookie TSPD) -- curl_cffi com impersonation resolve. Paginacao via query string em portugues (&pagina=N, nao "page"), preservando o tema= -- confirmado 93 URLs unicas em 7 paginas, batendo com totalElements=93 do payload RSC embutido no HTML. Fonte pequena -- max_paginas=7 cobre o arquivo inteiro. Data e titulo ja vem prontos e corretos na listagem. Texto completo reaproveita o fallback ja existente de extrair_texto_generico() (soup.find("article")) -- sem seletor novo nem extrator proprio.'
    WHERE nome_fonte = 'SECEX CONSENSO'
""")
print(f"[ok] linha 'SECEX CONSENSO' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta tcu_solucao_consensual em fontes_cobertas
# =============================================================================

fontes_lista = [f.strip() for f in fontes_atuais.split(",") if f.strip()]
if SOURCE_ID not in fontes_lista:
    fontes_lista.append(SOURCE_ID)
novo_valor = ", ".join(fontes_lista)

spark.sql(f"""
    UPDATE {TABELA_NOTEBOOKS}
    SET fontes_cobertas = '{novo_valor}'
    WHERE notebook_nome = '{NOTEBOOK_NOME}'
""")
print(f"[ok] fontes_cobertas de '{NOTEBOOK_NOME}' atualizado para: {novo_valor!r}")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'"))
display(spark.sql(f"SELECT notebook_nome, fontes_cobertas FROM {TABELA_NOTEBOOKS} WHERE notebook_nome = '{NOTEBOOK_NOME}'"))
