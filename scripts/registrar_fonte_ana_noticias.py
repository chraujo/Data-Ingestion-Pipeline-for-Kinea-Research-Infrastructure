# Databricks notebook source
# =============================================================================
# registrar_fonte_ana_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# (ver CLAUDE.md) para "ANA - Notícias": insere a linha em controle_fontes
# (status inicial "Não iniciada") e atualiza controle_notebooks.
#
# Diferença em relação ao caso ONS: ANA entrou no dispatcher GENÉRICO
# ingest-scraping (não é notebook próprio) -- o notebook e a task de job já
# existem e já rodam com o widget "fonte" no default "todas", então a nova
# entrada em CONFIGS_FONTES já está automaticamente coberta pela task
# "Ingest-scraping" existente no job "Ingest-news-INFRA". Não é preciso
# criar task nova nem linha nova em controle_tasks -- só atualizar
# fontes_cobertas em controle_notebooks pra refletir a fonte a mais.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"
TABELA_TASKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}tasks"

SOURCE_ID = "ana_noticias"
NOTEBOOK_NOME = "ingest-scraping"

# COMMAND ----------

# Trava de segurança: aborta se o source_id já existir.
ja_existe = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] nenhuma duplicata encontrada em controle_fontes; prosseguindo.")

# COMMAND ----------

# Confirma que o notebook compartilhado já está registrado (deveria estar,
# já que ingest-scraping é um dispatcher genérico pré-existente) e mostra a
# task correspondente em controle_tasks, só pra conferência -- nenhuma das
# duas precisa de linha nova.
linha_notebook = spark.sql(f"""
    SELECT notebook_nome, fontes_cobertas FROM {TABELA_NOTEBOOKS}
    WHERE notebook_nome = '{NOTEBOOK_NOME}'
""").collect()

if not linha_notebook:
    raise RuntimeError(f"notebook_nome='{NOTEBOOK_NOME}' não encontrado em {TABELA_NOTEBOOKS}; esperado já existir.")

fontes_atuais = linha_notebook[0]["fontes_cobertas"] or ""
print(f"[info] fontes_cobertas atual de '{NOTEBOOK_NOME}': {fontes_atuais!r}")

display(spark.sql(f"""
    SELECT job, nome_task, notebook, parametro_fonte, modo_execucao, horario_agendado
    FROM {TABELA_TASKS}
    WHERE notebook = '{NOTEBOOK_NOME}'
"""))

# COMMAND ----------

# Amostra de fontes do setor Saneamento já cadastradas — referência de
# taxonomia antes de decidir os valores abaixo.
display(
    spark.sql(f"""
        SELECT source_id, nome_fonte, tipo_categoria, setor, granularidade_conteudo,
               confidence_tier, metodo_captura, cadencia_real
        FROM {TABELA_FONTES}
        WHERE setor = 'Saneamento'
        LIMIT 10
    """)
)

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta ana_noticias em fontes_cobertas
# (sem duplicar, caso o script seja rodado de novo por engano).
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
# INSERT em controle_fontes
#
# tipo_categoria='Órgãos Públicos' e granularidade_conteudo='Notícia
# completa' por analogia direta com ons_noticias (mesmo tipo de entidade --
# agência federal, notícia institucional completa). confidence_tier
# deixado NULL -- não tenho amostra suficiente da taxonomia usada nesse
# campo pra escolher com confiança.
# =============================================================================

spark.sql(f"""
    INSERT INTO {TABELA_FONTES} (
        source_id, nome_fonte, tipo_categoria, setor, importancia_original,
        status, granularidade_conteudo, confidence_tier, metodo_captura,
        notebooks_responsaveis, tasks_responsaveis, cadencia_real, notas,
        ultima_execucao, docs_capturados, ultimo_erro, data_migracao
    ) VALUES (
        '{SOURCE_ID}', 'ANA - Notícias', 'Órgãos Públicos', 'Saneamento', NULL,
        'Não iniciada', 'Notícia completa', NULL,
        'Scraping HTML renderizado no servidor (Plone/gov.br) via dispatcher generico ingest-scraping, com paginacao propria (b_start:int)',
        '{NOTEBOOK_NOME}', 'Ingest-scraping', NULL,
        'URL da listagem e TEMPORARIA (periodo eleitoral 2026) -- revisar apos o periodo eleitoral, ver TODO no codigo do dispatcher (funcao listar_ana e entrada CONFIGS_FONTES). Compartilha notebook/task ja existentes de ingest-scraping (Acende Brasil, ANTT, Agesan, PSR, ANEEL, Agetransp) -- roda automaticamente no job Ingest-news-INFRA (task Ingest-scraping, fonte=todas), sem precisar de task nova. Sem filtro de relevancia na captura (proposital, fica para a etapa de NLP).',
        NULL, NULL, NULL, NULL
    )
""")
print(f"[ok] {SOURCE_ID} inserido em {TABELA_FONTES}")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'"))
display(spark.sql(f"SELECT notebook_nome, fontes_cobertas FROM {TABELA_NOTEBOOKS} WHERE notebook_nome = '{NOTEBOOK_NOME}'"))
