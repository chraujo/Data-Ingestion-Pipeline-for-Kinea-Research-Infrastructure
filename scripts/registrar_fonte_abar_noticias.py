# Databricks notebook source
# =============================================================================
# registrar_fonte_abar_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "ABAR - Acontece nas Agências". Diferente de ONS/ANP/ABEGAS/ANATEL,
# não existia nenhuma linha "ABAR" em controle_fontes (nem como placeholder
# do catálogo original de 79 fontes) -- INSERT de linha nova, não UPDATE.
# Confirmado antes de rodar: 0 linhas com nome_fonte/source_id contendo
# "abar", 0 colisão de source_id="abar_noticias".
#
# Importância definida diretamente ao registrar (instrução do usuário):
#   "Média" -- mesmo critério de ANA/ANP (agregador/generalista federal,
#   não uma agência estadual com feed direto como AGENERSA "Alta", nem uma
#   associação setorial predominantemente institucional como ABEGÁS
#   "Baixa"). ABAR cobre 87 agências associadas -- boa amplitude, mas
#   conteúdo variando de institucional a regulatório pontual das próprias
#   agências (repetido no campo "notas").
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "abar_noticias"
NOME_FONTE = "ABAR"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linhas_existentes = spark.sql(f"""
    SELECT source_id, nome_fonte FROM {TABELA_FONTES}
    WHERE nome_fonte ILIKE '%abar%' OR source_id ILIKE '%abar%'
""").collect()

if linhas_existentes:
    raise RuntimeError(f"Já existe linha relacionada a 'ABAR' em {TABELA_FONTES}: {linhas_existentes}; abortando (esperava INSERT novo, não UPDATE).")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] nenhuma linha 'ABAR' pré-existente, nenhum source_id duplicado; prosseguindo com INSERT.")

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
# INSERT em controle_fontes -- linha nova (sem placeholder pré-existente)
# =============================================================================

spark.sql(f"""
    INSERT INTO {TABELA_FONTES} (
        source_id, nome_fonte, tipo_categoria, setor, importancia_original,
        status, granularidade_conteudo, confidence_tier, metodo_captura,
        notebooks_responsaveis, tasks_responsaveis, cadencia_real, notas,
        ultima_execucao, docs_capturados, ultimo_erro, data_migracao,
        coberta_por, grupo_exibicao
    ) VALUES (
        '{SOURCE_ID}',
        '{NOME_FONTE}',
        'Associação Nacional de Agências Reguladoras (Agregador)',
        'Regulatório/Múltiplo',
        'Média',
        'Não iniciada',
        'N/D',
        'N/D',
        'Scraping HTML renderizado no servidor (WordPress, tema magazine familia Jannah/jeg) via dispatcher generico ingest-scraping, paginacao por path (/page/N)',
        '{NOTEBOOK_NOME}',
        'Ingest-scraping',
        'N/D',
        'Agregador nacional -- noticias de 87 agencias reguladoras associadas (saneamento, energia, transporte, recursos hidricos, estaduais e municipais), nao uma agencia especifica. Categoria "acontece-nas-agencias" tem ~1.042 posts -- max_paginas=5 no CONFIGS_FONTES cobre so os ~70 mais recentes por execucao. Armadilha do tema: bloco "hero" fixo (4 posts mais recentes, <h2>) se repete identico em toda pagina da categoria, junto da lista de fato paginada (<h3>) -- listar_abar() usa seletor sem restricao de tag (.jeg_post_title a) com dedup por URL para nao contar o hero de novo a cada pagina. Sem filtro de relevancia na captura (proposital, fica para a etapa de NLP): granularidade varia de evento institucional a decisao regulatoria pontual (revisao tarifaria, consulta publica, fiscalizacao).',
        NULL,
        0,
        NULL,
        NULL,
        NULL,
        NULL
    )
""")
print(f"[ok] linha 'ABAR' inserida com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta abar_noticias em fontes_cobertas
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
