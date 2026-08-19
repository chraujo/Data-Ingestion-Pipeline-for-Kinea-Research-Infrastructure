# Databricks notebook source
# =============================================================================
# registrar_fonte_cmse_google_news.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "CMSE" via Google News (consulta por termo nomeado, integrada
# direto em ingestores/google_news/ingest-news.ipynb — mesmo padrão já
# usado para "Projeto Notícias", sem notebook próprio). Já existia linha
# placeholder "CMSE" em controle_fontes (catálogo original, pensada
# originalmente para a busca filtrada no site do MME, nunca implementada
# -- source_id="—"). UPDATE na linha existente em vez de INSERT, mesmo
# padrão de ONS/ANP/ABEGAS/ANATEL/.../TCU/TELETIME -- NÃO cria fonte
# nova com outro nome.
#
# Esse dispatcher não tem source_id por entrada (todo item sai com
# source_id="linked_article" no metadado) nem chama
# atualizar_status_fonte() -- por isso o registro em controle_fontes é
# manual, via este script, mesmo padrão já usado para "Projeto Notícias"
# (docs_capturados/ultima_execucao ficam NULL de propósito, não há
# contagem automática por consulta individual).
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Órgãos Públicos"
#   importancia_original = "Alta" -- já vinha definida no catálogo
#   original, sem necessidade de reconferir.
#
# Campo ALTERADO por instrução explícita (não é preservação, é correção):
#   setor: "Energia" -> "Regulatório/Múltiplo" -- CMSE é ligado ao MME,
#   mas de caráter cross-setorial (mesmo raciocínio já usado para outras
#   correções explícitas de setor, ex. ANP).
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "google_news_cmse"
NOTEBOOK_NOME = "ingest-news"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'CMSE'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'CMSE' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'CMSE' encontrado, nenhum source_id duplicado; prosseguindo.")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_fontes
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        source_id = '{SOURCE_ID}',
        setor = 'Regulatório/Múltiplo',
        status = 'Coberta e validada em produção',
        metodo_captura = 'Google News (consulta por termo nomeado "CMSE"), integrada em ingestores/google_news/ingest-news.ipynb junto com as demais consultas de ativos nomeados -- mesmo padrao ja usado para Projeto Noticias',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = '—',
        notas = 'Query literal "CMSE" (sem variantes de nome). Validada isoladamente antes de integrar: resultados reais e relevantes numa janela de 30 dias (ex. "CMSE antecipa termicas do LRCap...", "CMSE discute acoes para mitigar super El Nino"), com algum ruido esperado de colisao de sigla com "Comando Militar do Sudeste" -- aceitavel, sem filtro de relevancia nesta etapa (fica para a etapa de NLP). Pipeline completo (RSS -> decode -> download -> extracao de texto) validado de ponta a ponta antes do registro. Notebook nao tem source_id por entrada (tudo sai como "linked_article") nem chama atualizar_status_fonte -- docs_capturados/ultima_execucao ficam NULL de proposito, sem contagem automatica por consulta individual, mesmo padrao ja usado para Projeto Noticias.'
    WHERE nome_fonte = 'CMSE'
""")
print(f"[ok] linha 'CMSE' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'"))
