# Databricks notebook source
# =============================================================================
# registrar_fonte_arpe_resolucoes.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "ARPE - Resoluções". Já existia linha placeholder "ARPE" em
# controle_fontes (catálogo original, nunca implementada -- source_id="—").
# UPDATE na linha existente em vez de INSERT, mesmo padrão de
# ONS/ANP/ABEGAS/ANATEL/Trata Brasil/CCEE -- NÃO cria fonte nova separada.
#
# Campos preservados do placeholder original (não sobrescritos):
#   tipo_categoria = "Agências Reguladoras Estaduais (PE)"
#   setor = "Saneamento"
#   importancia_original = "Alta" -- já vinha definida no catálogo
#   original, sem necessidade de reconferir.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "arpe_resolucoes"
NOTEBOOK_NOME = "ingest-pdfs-generico"

# COMMAND ----------

linha_placeholder = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'ARPE'
""").collect()

if len(linha_placeholder) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'ARPE' em {TABELA_FONTES}, achei {len(linha_placeholder)}; abortando.")

ja_existe_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'
""").collect()[0]["qtd"]

if ja_existe_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em {TABELA_FONTES}; abortando.")

print("[ok] 1 placeholder 'ARPE' encontrado, nenhum source_id duplicado; prosseguindo.")

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
        metodo_captura = 'Download direto de PDF via dispatcher generico ingest-PDF, listar_arpe_resolucoes() proprio -- titulo/data ficam num <div> de texto solto, separado do <a href=".pdf"> (Joomla com divs mal-fechados em cascata), pareados por ordem de aparicao no HTML',
        notebooks_responsaveis = '{NOTEBOOK_NOME}',
        tasks_responsaveis = 'Ingest-PDF',
        notas = 'Pagina unica (/legislacao/resolucoes-arpe), sem paginacao, cobre o historico completo desde a Resolucao Nº 001/2001 -- primeira execucao faz backfill de ~300 PDFs de uma vez, execucoes seguintes so pegam novas (dedup via manifesto). Pendencia conhecida (mesmo padrao das variacoes da Agesan-RS): ~25 resolucoes (a maioria das mais recentes de 2026, e algumas antigas esparsas) nao tem link .pdf direto -- o link vai pra uma pagina HTML propria com o texto integral embutido, sem PDF associado, fora do contrato atual de processar_pdf() (que baixa bytes e extrai via pypdf). Ficam de fora por ora, contadas no log do dispatcher como "ignoradas (sem PDF direto)".'
    WHERE nome_fonte = 'ARPE'
""")
print(f"[ok] linha 'ARPE' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta arpe_resolucoes em fontes_cobertas
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
