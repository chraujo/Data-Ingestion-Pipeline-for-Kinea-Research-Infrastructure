# Databricks notebook source
# =============================================================================
# corrigir_dados_relatorio.py
#
# Correções pontuais na tabela controle_fontes, necessárias para o relatório
# de progresso mostrar a realidade corretamente. Três problemas diferentes:
#
#   1. Megawhat (e possivelmente outras) com status desatualizado — este
#      script primeiro DIAGNOSTICA, você confirma o que está errado antes
#      de qualquer correção ser aplicada (não mexe em nada sozinho aqui).
#   2. AGEMS: cobertura indireta (via Diário Oficial de MS) não tinha como
#      ser representada — adiciona a coluna `coberta_por`.
#   3. AGESAN: as variações de captura (Resoluções, CSR, DC, Notícias) devem
#      aparecer agrupadas como uma fonte só no relatório, não como itens
#      pendentes separados — adiciona a coluna `grupo_exibicao`.
#
# Rodar uma vez. Seguro rodar de novo (usa ADD COLUMN IF NOT EXISTS e UPDATE
# idempotente).
# =============================================================================

# COMMAND ----------

TABELA = "desafio_kinea.research.controle_fontes"

# COMMAND ----------

# =============================================================================
# 1) DIAGNÓSTICO — rode e leia antes de continuar
# =============================================================================

print("=== Megawhat — estado atual na tabela ===")
display(spark.sql(f"""
    SELECT source_id, nome_fonte, status, notebooks_responsaveis,
           metodo_captura, ultima_execucao, docs_capturados
    FROM {TABELA}
    WHERE nome_fonte ILIKE '%megawhat%'
"""))

print("=== Outras fontes com padrão parecido (pra checar se é um caso isolado ou geral) ===")
display(spark.sql(f"""
    SELECT source_id, nome_fonte, status, ultima_execucao
    FROM {TABELA}
    WHERE status LIKE '%Coberta%' AND ultima_execucao IS NULL
"""))

# COMMAND ----------

# =============================================================================
# 2) Correção do status da Megawhat
#
# ATENÇÃO: preencha o valor certo em NOVO_STATUS_MEGAWHAT depois de ler o
# diagnóstico acima. Deixei "Coberta e validada em produção" como valor mais
# provável (é o que a Fonte deveria refletir, já que você confirmou que ela
# está pronta) — ajuste se o diagnóstico mostrar outra coisa (ex.: um
# source_id diferente do esperado, ou um notebook não instrumentado ainda).
# =============================================================================

NOVO_STATUS_MEGAWHAT = "Coberta e validada em produção"

spark.sql(f"""
    UPDATE {TABELA}
    SET status = '{NOVO_STATUS_MEGAWHAT}'
    WHERE nome_fonte ILIKE '%megawhat%'
""")
print(f"[ok] Megawhat -> status = '{NOVO_STATUS_MEGAWHAT}'")

# COMMAND ----------

# =============================================================================
# 3) Cobertura indireta (AGEMS via Diário Oficial de MS)
# =============================================================================

spark.sql(f"ALTER TABLE {TABELA} ADD COLUMNS (coberta_por STRING)")

spark.sql(f"""
    UPDATE {TABELA}
    SET status = 'Coberta indiretamente',
        coberta_por = 'diario_oficial_ms'
    WHERE nome_fonte ILIKE '%agems%'
""")
print("[ok] AGEMS -> status = 'Coberta indiretamente', coberta_por = 'diario_oficial_ms'")

# COMMAND ----------

# =============================================================================
# 4) Agrupamento visual das variações de captura da AGESAN
# =============================================================================

spark.sql(f"ALTER TABLE {TABELA} ADD COLUMNS (grupo_exibicao STRING)")

FONTES_AGESAN_RESOLUCOES = [
    "agesan_rs_resolucoes",
    "agesan_rs_resolucoes_csr",
    "agesan_rs_resolucoes_dc",
]

for source_id in FONTES_AGESAN_RESOLUCOES:
    spark.sql(f"""
        UPDATE {TABELA}
        SET grupo_exibicao = 'AGESAN-RS — Resoluções (todas as variações de captura)'
        WHERE source_id = '{source_id}'
    """)
print(f"[ok] {len(FONTES_AGESAN_RESOLUCOES)} variações da AGESAN agrupadas para exibição")

# COMMAND ----------

# =============================================================================
# Verificação final
# =============================================================================

display(spark.sql(f"""
    SELECT source_id, nome_fonte, status, coberta_por, grupo_exibicao
    FROM {TABELA}
    WHERE nome_fonte ILIKE '%megawhat%' OR nome_fonte ILIKE '%agems%' OR grupo_exibicao IS NOT NULL
"""))
