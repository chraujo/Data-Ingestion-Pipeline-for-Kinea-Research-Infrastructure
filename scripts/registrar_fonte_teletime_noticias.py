# Databricks notebook source
# =============================================================================
# registrar_fonte_teletime_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "TELETIME News - Notícias". Já existia linha placeholder
# "Teletime" em controle_fontes (catálogo original, nunca implementada --
# source_id="—"). UPDATE na linha existente em vez de INSERT, mesmo
# padrão de ONS/ANP/ABEGAS/ANATEL/Trata Brasil/CCEE/ABRACE/AESBE/TCU --
# NÃO cria fonte nova com outro nome.
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Mídia especializada"
#   setor = "Telecomunicações"
#   importancia_original = "Baixa" -- já vinha definida no catálogo
#   original (mídia especializada, não fonte institucional/reguladora
#   primária -- mesmo raciocínio já usado para ABEGÁS/ABRACE/AESBE), sem
#   necessidade de reconferir.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "teletime_noticias"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'Teletime'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'Teletime' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'Teletime' encontrado, nenhum source_id duplicado; prosseguindo.")

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
        metodo_captura = 'Scraping HTML renderizado no servidor (WordPress, tema tagDiv Newspaper) via dispatcher generico ingest-scraping, paginacao por path (/page/N)',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'robots.txt bloqueia ClaudeBot nominalmente (junto com GPTBot, CCBot, MJ12bot) -- usuario consultado e autorizou explicitamente prosseguir antes de qualquer requisicao. Sem bloqueio tecnico (WAF/anti-bot) -- httpx simples basta. Sem paywall confirmado (texto completo, sem truncamento). Data de publicacao ja vem pronta na listagem (time.td-module-date), com fallback por regex quando o atributo datetime vem vazio (primeiro item da pagina). Historico gigantesco (1.967 paginas) -- max_paginas=5 cobre so os ~180 mais recentes por execucao. Sem filtro de relevancia na captura (proposital, fica para a etapa de NLP).'
    WHERE nome_fonte = 'Teletime'
""")
print(f"[ok] linha 'Teletime' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta teletime_noticias em fontes_cobertas
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
