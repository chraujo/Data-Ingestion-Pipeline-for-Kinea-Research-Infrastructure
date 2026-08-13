# Databricks notebook source
# =============================================================================
# registrar_fonte_tratabrasil_blog.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "Instituto Trata Brasil - Blog". Já existia linha placeholder
# "Instituto Trata Brasil" em controle_fontes (catálogo original, nunca
# implementada -- source_id="—"). UPDATE na linha existente em vez de
# INSERT, mesmo padrão de ONS/ANP/ABEGAS/ANATEL.
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Sites especializados"
#   setor = "Saneamento"
#   importancia_original = "Média" -- já vinha definida no catálogo
#   original, sem necessidade de reconferir (mesmo critério das fontes
#   anteriores, quando o catálogo já traz um valor coerente).
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "tratabrasil_blog"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'Instituto Trata Brasil'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'Instituto Trata Brasil' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'Instituto Trata Brasil' encontrado, nenhum source_id duplicado; prosseguindo.")

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
        metodo_captura = 'Scraping HTML renderizado no servidor (WordPress + Elementor) via dispatcher generico ingest-scraping, paginacao por path (/blog/N)',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'Conteudo mais analitico/institucional (estudos, rankings, dados de investimento) do que noticia factual pontual de agencia reguladora. Listagem em div.e-loop-item (loop Elementor), texto completo em .elementor-widget-theme-post-content (acrescentado a SELETORES_CONTEUDO -- tema nao usa .entry-content). Historico medio (~300 posts / 30 paginas) -- max_paginas=4 no CONFIGS_FONTES cobre so os ~40 mais recentes por execucao. Sem filtro de relevancia na captura (proposital, fica para a etapa de NLP).'
    WHERE nome_fonte = 'Instituto Trata Brasil'
""")
print(f"[ok] linha 'Instituto Trata Brasil' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta tratabrasil_blog em fontes_cobertas
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
