# Databricks notebook source
# =============================================================================
# registrar_fonte_anp_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "ANP - Notícias e Comunicados": diferente de ONS/ANA, já existia uma
# linha placeholder "ANP" em controle_fontes (do catálogo original de 79
# fontes, nunca implementada -- source_id="—"). Em vez de INSERT (que
# criaria uma duplicata, o mesmo problema que já precisou ser corrigido
# para ANA/ONS/Pipeline -- ver commit c742d8a), este script faz UPDATE
# nessa linha existente.
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Agências Reguladoras Federais"
#   importancia_original = "Média"
#
# Campo ajustado: setor estava "Gás" no catálogo original; instrução
# explícita do usuário para esta fonte foi "setor Energia" -- prevalece a
# instrução mais recente.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "anp_noticias"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

# Trava de segurança: confirma que existe exatamente 1 linha placeholder
# "ANP" pra atualizar, e que nenhuma outra linha já usa este source_id.
linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'ANP'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'ANP' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'ANP' encontrado, nenhum source_id duplicado; prosseguindo.")

# COMMAND ----------

# Confirma que o notebook compartilhado já está registrado.
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
# UPDATE em controle_fontes -- atualiza a linha placeholder existente em vez
# de inserir uma nova.
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        source_id = '{SOURCE_ID}',
        setor = 'Energia',
        status = 'Não iniciada',
        metodo_captura = 'Scraping HTML renderizado no servidor (Plone/gov.br) via dispatcher generico ingest-scraping, com paginacao propria (b_start:int) -- reaproveita listar_ana()',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'Endereco permanente (sem URL temporaria, diferente da ANA). Historico grande (~450 itens / ~15 paginas) -- max_paginas=5 no CONFIGS_FONTES cobre so os ~150 mais recentes por execucao. Sem filtro de relevancia na captura (proposital, fica para a etapa de NLP): lista mistura ruido administrativo (fechamento de protocolo, reuniao de diretoria) com conteudo regulatorio relevante (fiscalizacao de precos, leiloes, consultas publicas). setor era "Gas" no catalogo original -- atualizado para "Energia" por instrucao explicita ao registrar.'
    WHERE nome_fonte = 'ANP'
""")
print(f"[ok] linha 'ANP' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta anp_noticias em fontes_cobertas
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
