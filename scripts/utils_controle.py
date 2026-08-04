# Databricks notebook source
# =============================================================================
# utils_controle.py
#
# Função compartilhada para os notebooks dispatchers atualizarem o status
# real de cada fonte na tabela de controle, toda vez que processarem algo.
#
# Uso, no final de cada dispatcher (ingest-news-rss-generico,
# ingest-scraping-generico, ingest-pdfs-generico, etc.), depois de processar
# uma fonte:
#
#     %run ../scripts/utils_controle
#
#     atualizar_status_fonte(
#         source_id="agencia_infra",
#         sucesso=True,
#         docs_capturados=12,
#     )
#
# Em caso de erro (ex.: dentro de um try/except em volta do processamento
# da fonte):
#
#     atualizar_status_fonte(
#         source_id="agencia_infra",
#         sucesso=False,
#         docs_capturados=0,
#         erro=str(e),
#     )
# =============================================================================

# COMMAND ----------

CATALOGO_CONTROLE = "desafio_kinea"
SCHEMA_CONTROLE = "research"
PREFIXO_CONTROLE = "controle_"


def atualizar_status_fonte(
    source_id: str,
    sucesso: bool,
    docs_capturados: int = 0,
    erro: str = None,
):
    """
    Atualiza o status real de uma fonte na tabela de controle, refletindo
    o que de fato aconteceu na última execução do dispatcher.

    Não cria linha nova se o source_id não existir na tabela (o MERGE só
    tem cláusula WHEN MATCHED) — isso é proposital: toda fonte que um
    dispatcher processa já deveria ter sido cadastrada na migração inicial
    ou manualmente. Se aparecer um source_id novo sem match, o comando
    simplesmente não altera nada, silenciosamente — vale conferir com
    um SELECT se isso acontecer sem explicação.
    """
    status = "Coberta e validada em produção" if sucesso else "Erro"

    # Escapa aspas simples no texto do erro para não quebrar o SQL
    # (proteção simples contra erro malformado, não input malicioso).
    erro_sql = "NULL" if erro is None else "'" + str(erro).replace("'", "''") + "'"

    spark.sql(f"""
        MERGE INTO {CATALOGO_CONTROLE}.{SCHEMA_CONTROLE}.{PREFIXO_CONTROLE}fontes AS t
        USING (SELECT '{source_id}' AS source_id) AS s
        ON t.source_id = s.source_id
        WHEN MATCHED THEN UPDATE SET
            status = '{status}',
            ultima_execucao = current_timestamp(),
            docs_capturados = {int(docs_capturados)},
            ultimo_erro = {erro_sql}
    """)
    print(f"[controle] {source_id}: status='{status}', docs={docs_capturados}" + (f", erro={erro}" if erro else ""))
