# Databricks notebook source
# =============================================================================
# corrigir_source_id_duplicado.py
#
# Migração ÚNICA (roda uma vez). Corrige um problema de dados encontrado na
# tabela controle_fontes: 5 fontes diferentes (Valor/Pipeline, Estadão,
# O Globo, PPI, Moody's) foram migradas do Excel com o mesmo source_id
# literal "site_page" — copiado do mesmo template de "site inteiro" sem
# ser customizado por fonte no código original de cada notebook.
#
# Isso é perigoso para o MERGE INTO usado por atualizar_status_fonte():
# como ele casa por source_id, rodar o dispatcher de uma dessas fontes
# atualizaria as 5 linhas de uma vez, sobrescrevendo o status das outras 4.
#
# Depois de rodar este script, cada uma passa a ter um source_id único e
# estável, que já bate com o que foi hardcoded nos notebooks correspondentes
# (ingest-news-pipeline.ipynb e ingest-news-PPI-fixed.ipynb, editados junto
# com esta migração; Estadão, O Globo e Moody's ainda precisam do mesmo
# tratamento quando esses notebooks forem revisados).
# =============================================================================

# COMMAND ----------

TABELA = "desafio_kinea.research.controle_fontes"

# nome_fonte -> novo source_id único
CORRECOES = {
    "Valor": "valor_pipeline",
    "Estadão": "estadao",
    "O Globo": "oglobo",
    "PPI": "ppi_gov_br",
    "Moody's": "moodys_local",
}

for nome_fonte, novo_source_id in CORRECOES.items():
    nome_escapado = nome_fonte.replace("'", "''")
    spark.sql(f"""
        UPDATE {TABELA}
        SET source_id = '{novo_source_id}'
        WHERE nome_fonte = '{nome_escapado}'
    """)
    print(f"[ok] {nome_fonte} -> source_id = '{novo_source_id}'")

# COMMAND ----------

# Verificação: não deve sobrar nenhum source_id duplicado na tabela inteira.
display(
    spark.sql(f"""
        SELECT source_id, COUNT(*) AS qtd, collect_list(nome_fonte) AS fontes
        FROM {TABELA}
        WHERE source_id IS NOT NULL AND source_id != '—'
        GROUP BY source_id
        HAVING COUNT(*) > 1
    """)
)
