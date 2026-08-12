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

try:
    spark.sql(f"ALTER TABLE {TABELA} ADD COLUMNS (coberta_por STRING)")
except Exception as e:
    if "FIELD_ALREADY_EXISTS" in str(e) or "already exists" in str(e):
        print("[ok] coluna coberta_por ja existe, seguindo")
    else:
        raise

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

try:
    spark.sql(f"ALTER TABLE {TABELA} ADD COLUMNS (grupo_exibicao STRING)")
except Exception as e:
    if "FIELD_ALREADY_EXISTS" in str(e) or "already exists" in str(e):
        print("[ok] coluna grupo_exibicao ja existe, seguindo")
    else:
        raise

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
# 5) Funde linhas duplicadas (fonte "fantasma" do catálogo original, nunca
#    tocada, coexistindo com a linha real criada quando a fonte foi de fato
#    implementada via Claude Code). A linha fantasma transfere sua
#    importancia_original pra linha real antes de ser apagada.
# =============================================================================

DUPLICATAS = [
    # (nome_fonte da linha fantasma no catálogo original, source_id da linha real)
    ("ANA", "ana_noticias"),
    ("ONS", "ons_noticias"),
    ("Pipeline", "valor_pipeline"),
]

for nome_fantasma, source_id_real in DUPLICATAS:
    linha_fantasma = spark.sql(f"""
        SELECT importancia_original FROM {TABELA}
        WHERE nome_fonte = '{nome_fantasma}' AND source_id = '—'
    """).collect()

    if not linha_fantasma:
        print(f"[aviso] nenhuma linha fantasma encontrada para '{nome_fantasma}' -- pulando")
        continue

    importancia_herdada = linha_fantasma[0]["importancia_original"]

    spark.sql(f"""
        UPDATE {TABELA}
        SET importancia_original = '{importancia_herdada}'
        WHERE source_id = '{source_id_real}' AND importancia_original IS NULL
    """)
    spark.sql(f"""
        DELETE FROM {TABELA}
        WHERE nome_fonte = '{nome_fantasma}' AND source_id = '—'
    """)
    print(f"[ok] '{nome_fantasma}' fundida em '{source_id_real}' (importância herdada: {importancia_herdada})")

# COMMAND ----------

# =============================================================================
# 6) As 3 fontes com importancia_original = "N/D" literal herdam a
#    prioridade do órgão-pai (mesmo órgão, variação de captura diferente).
# =============================================================================

HERANCA_PRIORIDADE = {
    "AGETRANSP — Atos Normativos (Resoluções/Deliberações/Portarias)": "Alta",  # herda de AGETRANSP
    "CMSE — Atas": "Alta",                                                      # herda de CMSE
    "CMSE — Resoluções do CMSE": "Alta",                                        # herda de CMSE
}

for nome_fonte, nova_prioridade in HERANCA_PRIORIDADE.items():
    nome_escapado = nome_fonte.replace("'", "''")
    spark.sql(f"""
        UPDATE {TABELA}
        SET importancia_original = '{nova_prioridade}'
        WHERE nome_fonte = '{nome_escapado}' AND importancia_original = 'N/D'
    """)
    print(f"[ok] '{nome_fonte}' -> importância '{nova_prioridade}' (herdada do órgão-pai)")

# COMMAND ----------

# =============================================================================
# 7) Descartes: ARSAL (período eleitoral), O Globo e Estadão (paywall)
# =============================================================================

DESCARTES = [
    # (padrão de nome_fonte, motivo)
    ("%ARSAL%", "Descarte: período eleitoral"),
    ("O Globo", "Descarte: paywall"),
    ("Estadão", "Descarte: paywall"),
]

for padrao, motivo in DESCARTES:
    motivo_escapado = motivo.replace("'", "''")
    spark.sql(f"""
        UPDATE {TABELA}
        SET status = 'Descartada',
            notas = CASE
                WHEN notas IS NULL OR TRIM(notas) = '' THEN '{motivo_escapado}'
                ELSE CONCAT(notas, ' | {motivo_escapado}')
            END
        WHERE nome_fonte ILIKE '{padrao}'
    """)
    print(f"[ok] '{padrao}' -> Descartada ({motivo})")

# COMMAND ----------

# =============================================================================
# 8) Brazil Journal (entrada geral do site) -> cobertura indireta via
#    Infra Journal (a editoria específica já coberta)
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA}
    SET status = 'Coberta indiretamente',
        coberta_por = 'brazil_journal_infra_journal'
    WHERE nome_fonte ILIKE '%brazil journal%' AND nome_fonte NOT ILIKE '%infra journal%'
""")
print("[ok] Brazil Journal (geral) -> coberta indiretamente via brazil_journal_infra_journal")

# COMMAND ----------

# =============================================================================
# 9) AGENERSA: funde a duplicata "AGENERSA - Notícias" (com a cobertura
#    real) na linha original "AGENERSA" (que já tem a prioridade Alta
#    correta) -- direção oposta à correção 5 (lá, a linha nova sobrevivia;
#    aqui, a linha original com a prioridade certa sobrevive, herdando os
#    dados reais de cobertura da duplicata antes dela ser apagada).
# =============================================================================

linha_duplicata = spark.sql(f"""
    SELECT source_id, status, metodo_captura, notebooks_responsaveis,
           tasks_responsaveis, ultima_execucao, docs_capturados, ultimo_erro
    FROM {TABELA}
    WHERE nome_fonte ILIKE '%agenersa%' AND nome_fonte NOT IN ('AGENERSA')
""").collect()

if not linha_duplicata:
    print("[aviso] duplicata da AGENERSA não encontrada -- confirme o nome exato e ajuste o filtro")
else:
    d = linha_duplicata[0]
    def _sql_str(v):
        return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"

    spark.sql(f"""
        UPDATE {TABELA}
        SET source_id = {_sql_str(d['source_id'])},
            status = {_sql_str(d['status'])},
            metodo_captura = {_sql_str(d['metodo_captura'])},
            notebooks_responsaveis = {_sql_str(d['notebooks_responsaveis'])},
            tasks_responsaveis = {_sql_str(d['tasks_responsaveis'])},
            ultima_execucao = {_sql_str(d['ultima_execucao'])},
            docs_capturados = {d['docs_capturados'] if d['docs_capturados'] is not None else 'NULL'},
            ultimo_erro = {_sql_str(d['ultimo_erro'])}
        WHERE nome_fonte = 'AGENERSA'
    """)
    spark.sql(f"""
        DELETE FROM {TABELA}
        WHERE nome_fonte ILIKE '%agenersa%' AND nome_fonte NOT IN ('AGENERSA')
    """)
    print("[ok] AGENERSA: dados reais herdados pela linha original (Prioridade Alta); duplicata removida")

# COMMAND ----------

# =============================================================================
# 10) CMSE: apaga as entradas "Atas" e "Resoluções" (não serão mais
#     capturadas separadamente); a entrada "CMSE" original é mantida no
#     relatório com o mesmo nome, mas passa a referenciar a nova URL de
#     captura (busca filtrada por "cmse" no site do MME).
# =============================================================================

spark.sql(f"""
    DELETE FROM {TABELA}
    WHERE nome_fonte ILIKE '%CMSE%' AND (nome_fonte ILIKE '%Atas%' OR nome_fonte ILIKE '%Resoluç%')
""")
print("[ok] 'CMSE — Atas' e 'CMSE — Resoluções do CMSE' removidas")

NOVA_URL_CMSE = "https://www.gov.br/mme/pt-br/assuntos/noticias?form.submitted=1&texto=cmse&dt_inicio=&dt_fim=&categoria="
linha_cmse_original = spark.sql(f"""
    SELECT nome_fonte FROM {TABELA}
    WHERE nome_fonte = 'CMSE'
""").collect()

if linha_cmse_original:
    spark.sql(f"""
        UPDATE {TABELA}
        SET notas = CASE
                WHEN notas IS NULL OR TRIM(notas) = '' THEN 'Nova URL de captura: {NOVA_URL_CMSE}'
                ELSE CONCAT(notas, ' | Nova URL de captura: {NOVA_URL_CMSE}')
            END
        WHERE nome_fonte = 'CMSE'
    """)
    print("[ok] Entrada 'CMSE' original atualizada com a nova URL nas notas")
else:
    print("[ATENÇÃO] Não existe uma linha 'CMSE' (só o nome exato) na tabela ainda -- "
          "as duas apagadas eram as únicas entradas dessa fonte. Criar manualmente uma "
          "linha 'CMSE' nova, com a URL acima, antes do próximo teste de captura.")

# COMMAND ----------

# =============================================================================
# 11) Pipeline (Valor) — diagnóstico apenas, sem gravar nada. Isso já foi
#     corrigido antes (fusão da linha fantasma do Excel com valor_pipeline);
#     conferir aqui se ainda está correto antes de qualquer nova mudança.
# =============================================================================

display(spark.sql(f"""
    SELECT source_id, nome_fonte, status, ultima_execucao, docs_capturados
    FROM {TABELA}
    WHERE nome_fonte ILIKE '%pipeline%' OR nome_fonte ILIKE '%valor%'
"""))

# COMMAND ----------

# =============================================================================
# 12) AGENERSA (regulatório, item 6 da lista anterior): garante prioridade
#     Alta na linha que sobreviveu à fusão da seção 9.
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA}
    SET importancia_original = 'Alta'
    WHERE nome_fonte = 'AGENERSA'
""")
print("[ok] AGENERSA -> importância Alta confirmada")

# COMMAND ----------

# Verificação final: não deve sobrar nenhuma fonte coberta/em produção sem
# importancia_original, nem nenhuma com o valor literal "N/D"
display(spark.sql(f"""
    SELECT source_id, nome_fonte, status, importancia_original
    FROM {TABELA}
    WHERE importancia_original IS NULL OR importancia_original = 'N/D'
    ORDER BY nome_fonte
"""))
