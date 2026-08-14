# Databricks notebook source
# =============================================================================
# registrar_investigacao_agrese_saneamento.py
#
# Registro ÚNICO (roda uma vez). Atualiza o placeholder "AGRESE" (catálogo
# original, já existia em controle_fontes com status='Não iniciada') para
# refletir que a investigação da Fase 1 já começou, mas ainda não foi
# validada -- feed RSS funciona, mas as URLs de notícia (tanto o permalink
# antigo do WordPress quanto o caminho novo em se.gov.br/agencia) devolvem
# 301 para página genérica sem conteúdo. Detalhe completo da investigação
# em MENSAGENS.md (seção "## Bloqueios").
#
# UPDATE, não INSERT -- preserva tipo_categoria, setor e
# importancia_original já presentes no placeholder original.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"

NOME_FONTE = "AGRESE"
NOVO_STATUS = "Iniciada — não validada"

NOTAS = (
    "Investigação da Fase 1 iniciada em 14/08/2026: feed RSS nativo "
    "(agrese.se.gov.br/feed/) funciona e lista os itens normalmente, mas "
    "as URLs de cada notícia devolvem HTTP 301 para uma página genérica "
    "sem conteúdo -- tanto o permalink antigo do WordPress "
    "(agrese.se.gov.br/[slug]/) quanto o caminho novo no portal central do "
    "governo de Sergipe (www.se.gov.br/agencia/noticias/governo/[slug]) "
    "estão quebrados no momento. Google já indexou conteúdo real nesse "
    "portal no passado, o que sugere instabilidade atual do lado do "
    "governo de Sergipe, não bloqueio deliberado. Ainda não validada -- "
    "vale retestar em algumas semanas antes de decidir se encaixa no "
    "dispatcher genérico de RSS (ingest-news-rss-infra, mesmo padrão da "
    "ARISB-MG) ou se precisa virar bloqueio formal. Detalhe completo em "
    "MENSAGENS.md, seção Bloqueios."
)

# COMMAND ----------

linha_atual = spark.sql(f"""
    SELECT nome_fonte, status FROM {TABELA_FONTES} WHERE nome_fonte = '{NOME_FONTE}'
""").collect()

if len(linha_atual) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha '{NOME_FONTE}' em {TABELA_FONTES}, achei {len(linha_atual)}; abortando.")

print(f"[info] status atual de '{NOME_FONTE}': {linha_atual[0]['status']!r} -> {NOVO_STATUS!r}")

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        status = '{NOVO_STATUS}',
        notas = '{NOTAS}'
    WHERE nome_fonte = '{NOME_FONTE}'
""")
print(f"[ok] linha '{NOME_FONTE}' atualizada.")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT source_id, nome_fonte, status, notas FROM {TABELA_FONTES} WHERE nome_fonte = '{NOME_FONTE}'"))
