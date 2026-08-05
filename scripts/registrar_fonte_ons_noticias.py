# Databricks notebook source
# =============================================================================
# registrar_fonte_ons_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# (ver CLAUDE.md) para "ONS - Notícias": insere a linha em controle_fontes
# (status inicial "Não iniciada"), em controle_notebooks e em controle_tasks.
#
# A task "Ingest-ONS" referenciada abaixo em controle_tasks é adicionada ao
# job "Ingest-news-INFRA" (job_id 1059728460076257) separadamente, via
# `databricks jobs update` — este script só registra o metadado; não mexe
# no job em si.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"
TABELA_TASKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}tasks"

SOURCE_ID = "ons_noticias"
NOTEBOOK_NOME = "ingest-news-ons"
NOME_TASK = "Ingest-ONS"

# COMMAND ----------

# Trava de segurança: aborta se o source_id já existir (regra de ouro do
# CLAUDE.md — source_id precisa ser único de verdade).
ja_existe = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

ja_existe_nb = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_NOTEBOOKS} WHERE notebook_nome = '{NOTEBOOK_NOME}'
""").collect()[0]["qtd"]

if ja_existe_nb > 0:
    raise RuntimeError(f"notebook_nome='{NOTEBOOK_NOME}' já existe em {TABELA_NOTEBOOKS}; abortando.")

print("[ok] nenhuma duplicata encontrada; prosseguindo.")

# COMMAND ----------

# Amostra de fontes do setor Energia já cadastradas — só para conferência
# manual da taxonomia usada em tipo_categoria/granularidade_conteudo/
# confidence_tier antes de eventualmente ajustar os NULLs deixados abaixo.
display(
    spark.sql(f"""
        SELECT source_id, nome_fonte, tipo_categoria, setor, granularidade_conteudo,
               confidence_tier, metodo_captura, cadencia_real
        FROM {TABELA_FONTES}
        WHERE setor = 'Energia'
        LIMIT 10
    """)
)

# COMMAND ----------

# =============================================================================
# INSERT em controle_fontes
#
# Campos deixados NULL de propósito (tipo_categoria, importancia_original,
# granularidade_conteudo, confidence_tier): essa fonte não veio do catálogo
# Excel original, então não tenho uma classificação confiável na mesma
# taxonomia das fontes migradas — fica para revisão manual em vez de
# adivinhar um valor que poderia destoar do padrão já em uso.
# =============================================================================

spark.sql(f"""
    INSERT INTO {TABELA_FONTES} (
        source_id, nome_fonte, tipo_categoria, setor, importancia_original,
        status, granularidade_conteudo, confidence_tier, metodo_captura,
        notebooks_responsaveis, tasks_responsaveis, cadencia_real, notas,
        ultima_execucao, docs_capturados, ultimo_erro, data_migracao
    ) VALUES (
        '{SOURCE_ID}', 'ONS - Notícias', NULL, 'Energia', NULL,
        'Não iniciada', NULL, NULL,
        'API REST interna do site (SharePoint client-side; sem HTML pra raspar) via httpx/curl_cffi, sem Selenium',
        '{NOTEBOOK_NOME}', 'Ingest-ONS', NULL,
        'Fase 1 testada em teste_ons_noticias.ipynb; validada localmente contra o site real (30 itens listados, texto completo extraido). Host precisa ser www.ons.org.br (sem www da 404). Endpoints descobertos lendo wp_noticias.js / wp_noticiasDetalhe.js.',
        NULL, NULL, NULL, NULL
    )
""")
print(f"[ok] {SOURCE_ID} inserido em {TABELA_FONTES}")

# COMMAND ----------

# =============================================================================
# INSERT em controle_notebooks
# =============================================================================

spark.sql(f"""
    INSERT INTO {TABELA_NOTEBOOKS} (
        notebook_nome, caminho_repo, padrao_arquitetura, parametrizado,
        fontes_cobertas, dependencias_externas, notas
    ) VALUES (
        '{NOTEBOOK_NOME}', 'ingestores/ENERGIA/ingest-news-ons.ipynb',
        'Notebook próprio (fonte única, sem widget) -- API REST, não HTML',
        'Não',
        '{SOURCE_ID}',
        'Nenhuma (API pública sem autenticação; httpx + curl_cffi puro)',
        'Não encaixa nos 3 dispatchers genericos porque o HTML da listagem/detalhe e renderizado client-side (SharePoint) e nao contem os dados -- consumido via endpoints REST do proxy do site.'
    )
""")
print(f"[ok] {NOTEBOOK_NOME} inserido em {TABELA_NOTEBOOKS}")

# COMMAND ----------

# =============================================================================
# INSERT em controle_tasks
#
# Reflete a task que está sendo adicionada ao job "Ingest-news-INFRA"
# (git_source, branch main, cluster existente "Cluster Desafio").
# =============================================================================

spark.sql(f"""
    INSERT INTO {TABELA_TASKS} (
        job, nome_task, notebook, parametro_fonte, modo_execucao,
        cluster, depends_on, horario_agendado
    ) VALUES (
        'Ingest-news-INFRA', '{NOME_TASK}', '{NOTEBOOK_NOME}', NULL,
        'Task em job (git_source, branch main)',
        'Cluster Desafio (existing_cluster_id 0411-142020-dxdkinpz)', NULL,
        '07:45 America/Sao_Paulo (cron 46 45 7 * * ?)'
    )
""")
print(f"[ok] {NOME_TASK} inserido em {TABELA_TASKS}")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'"))
display(spark.sql(f"SELECT * FROM {TABELA_NOTEBOOKS} WHERE notebook_nome = '{NOTEBOOK_NOME}'"))
display(spark.sql(f"SELECT * FROM {TABELA_TASKS} WHERE nome_task = '{NOME_TASK}'"))
