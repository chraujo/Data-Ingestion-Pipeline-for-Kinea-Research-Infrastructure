# Databricks notebook source
# =============================================================================
# registrar_fonte_anatel_noticias.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "ANATEL - Notícias". Placeholder "ANATEL" já existia em
# controle_fontes (catálogo original, nunca implementada -- source_id="—").
# UPDATE na linha existente em vez de INSERT (mesmo padrão de ONS/ANP/
# ABEGÁS).
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Agências Reguladoras Federais"
#   setor = "Telecomunicações"
#   importancia_original = "Média" -- confirmado explicitamente pelo
#   usuário ao pedir o registro, sem precisar reconferir contra o
#   catálogo (bateu com o que já estava lá).
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "anatel_noticias"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'ANATEL'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'ANATEL' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'ANATEL' encontrado, nenhum source_id duplicado; prosseguindo.")

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
        metodo_captura = 'API REST publica do Plone 6/Volto (++api++/@search) para listagem, HTML server-side renderizado (extrair_texto_generico, seletor #page-document) para o texto de cada noticia -- via dispatcher generico ingest-scraping',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'Arquitetura Plone 6/Volto (frontend React, blocos), diferente de ANA/ANP (Plone classico) apesar da mesma familia gov.br -- pagina de listagem publica e so a casca do SearchBlock React, sem itens; listagem via API REST publica (path relativo a raiz do site, sem prefixo /anatel/, senao devolve 0 silenciosamente). Historico pequeno (16 itens), sem paginacao necessaria. Sem filtro de relevancia na captura (proposital, fica para a etapa de NLP).'
    WHERE nome_fonte = 'ANATEL'
""")
print(f"[ok] linha 'ANATEL' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta anatel_noticias em fontes_cobertas
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
