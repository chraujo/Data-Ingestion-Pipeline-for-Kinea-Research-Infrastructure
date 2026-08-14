# Databricks notebook source
# =============================================================================
# registrar_fonte_aesbe_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "AESBE - Notícias". Já existia linha placeholder "AESBE" em
# controle_fontes (catálogo original, nunca implementada -- source_id=
# "—"). UPDATE na linha existente em vez de INSERT, mesmo padrão de
# ONS/ANP/ABEGAS/ANATEL/Trata Brasil/CCEE/ABRACE.
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Associações Setoriais (Saneamento)"
#   setor = "Saneamento"
#   importancia_original = "Baixa" -- já vinha definida no catálogo
#   original, consistente com outras associações setoriais já cadastradas
#   (ABEGÁS e ABRACE também "Baixa"), sinal forte o suficiente pra não
#   reclassificar.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "aesbe_noticias"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'AESBE'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'AESBE' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'AESBE' encontrado, nenhum source_id duplicado; prosseguindo.")

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
        metodo_captura = 'Scraping HTML renderizado no servidor (WordPress + Elementor) via dispatcher generico ingest-scraping, paginacao por path (/N/)',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'Listagem tem dois mecanismos de duplicacao empilhados: widget hero (2 posts mais recentes) sobreposto a lista paginada de verdade, e cada item da lista paginada repete titulo/link 3x internamente (variantes responsivas desktop/tablet/mobile do mesmo bloco Elementor) -- resolvido com dedup por URL + pegar so o primeiro titulo por article. Data de publicacao nao aparece na listagem, so na pagina individual (texto puro DD/MM/YYYY, sem atributo datetime). Pagina individual tem 4 <h1> (so o primeiro e o titulo de verdade, resto e widget lateral de newsletter) -- extrair_titulo=None no CONFIGS_FONTES por seguranca. Historico medio (10 paginas / ~140 posts) -- max_paginas=4 cobre so os ~48 mais recentes por execucao. Conteudo institucional/advocacy do setor (propostas de politica publica, eventos, camaras tecnicas, premios) -- sem filtro de relevancia na captura (proposital, fica para a etapa de NLP).'
    WHERE nome_fonte = 'AESBE'
""")
print(f"[ok] linha 'AESBE' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta aesbe_noticias em fontes_cobertas
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
