# Databricks notebook source
# =============================================================================
# registrar_fonte_ccee_atas_diretoria.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "CCEE - Atas da Diretoria". Já existia linha placeholder "CCEE" em
# controle_fontes (catálogo original, nunca implementada -- source_id="—").
# UPDATE na linha existente em vez de INSERT, mesmo padrão de
# ONS/ANP/ABEGAS/ANATEL/Trata Brasil -- NÃO cria fonte nova separada.
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Órgãos Públicos"
#   setor = "Energia"
#   importancia_original = "Alta" -- já vinha definida no catálogo
#   original, sem necessidade de reconferir.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "ccee_atas_diretoria"
NOTEBOOK_NOME = "ingest-pdfs-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'CCEE'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'CCEE' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'CCEE' encontrado, nenhum source_id duplicado; prosseguindo.")

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
        metodo_captura = 'Download direto de PDF (Liferay, portlet CCEEAcervoPortlet) via dispatcher generico ingest-PDF, com curl_cffi (impersonation de TLS) para contornar WAF -- listar_ccee() proprio para titulo/data especificos de cada ata',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-PDF',
        notas = 'Pagina bloqueada por WAF para requisicao simples (403 "acesso bloqueado") -- curl_cffi com impersonation de TLS resolve, mesmos headers ja usados no resto do projeto. HTML default (sem filtro) ja vem com a janela recente pronta (~30 dias, via filtro de data setado pelo JS da propria pagina antes do primeiro paint) -- sem precisar do endpoint AJAX (Liferay serveResource) que o botao "Filtrar" dispara. Esse endpoint AJAX existe e cobre um arquivo bem maior (~460 documentos, 2020-2026), mas mistura Atas da Diretoria com Atas do Conselho de Administracao sem filtro de tipo -- nao usado na captura recorrente, so documentado no notebook de teste para eventual backfill futuro.'
    WHERE nome_fonte = 'CCEE'
""")
print(f"[ok] linha 'CCEE' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta ccee_atas_diretoria em fontes_cobertas
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
