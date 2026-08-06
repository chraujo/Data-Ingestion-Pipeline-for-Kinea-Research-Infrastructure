# Databricks notebook source
# =============================================================================
# descartar_brasil_energia_canal_energia.py
#
# Correção ÚNICA (roda uma vez). Marca "Brasil Energia" e "Canal Energia"
# como descartadas por paywall: atualiza o status em controle_fontes e
# registra o motivo em controle_bloqueios, seguindo o mesmo padrão já usado
# para UOL (blq_002, também descartada por paywall confirmado).
#
# As duas fontes nunca chegaram a ter notebook/dispatcher (source_id="—",
# notebooks_responsaveis="—") -- não há nenhum código de scraping a
# remover, só o registro de controle.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_BLOQUEIOS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios"

NOMES_FONTES = ["Brasil Energia", "Canal Energia"]

# COMMAND ----------

# Trava de segurança: aborta se já existir bloqueio registrado pra alguma
# das duas (evita duplicar se o script rodar de novo por engano).
placeholders = ", ".join(f"'{n}'" for n in NOMES_FONTES)
ja_existe = spark.sql(f"""
    SELECT nome FROM {TABELA_BLOQUEIOS}
    WHERE nome IN ({placeholders})
""").collect()

if ja_existe:
    nomes_existentes = [r["nome"] for r in ja_existe]
    raise RuntimeError(f"Já existe bloqueio registrado para: {nomes_existentes}; abortando.")

print("[ok] nenhum bloqueio duplicado encontrado; prosseguindo.")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_fontes
#
# status='Descartada', mesmo valor já usado para UOL (nome_fonte='UOL'),
# que foi descartada pelo mesmo motivo (paywall confirmado). Diferente de
# 'Bloqueada', reservado para bloqueio técnico (ex.: robots.txt da
# AGENERSA) -- aqui o motivo é editorial/comercial (assinatura paga), não
# uma restrição técnica de acesso.
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET status = 'Descartada'
    WHERE nome_fonte IN ({placeholders})
""")
print(f"[ok] status='Descartada' aplicado a: {NOMES_FONTES}")

# COMMAND ----------

# =============================================================================
# INSERT em controle_bloqueios
# =============================================================================

proximo_id = spark.sql(f"""
    SELECT MAX(CAST(SUBSTRING(id, 5) AS INT)) AS max_num FROM {TABELA_BLOQUEIOS}
    WHERE id LIKE 'blq_%'
""").collect()[0]["max_num"] + 1

NOTA_COMUM = (
    "Citada junto com a outra no prompt de gera_config.ipynb como fonte "
    "esperada pelo pipeline de briefing (lista de fontes pré-capturadas) "
    "-- esse texto fica desatualizado agora que a fonte foi descartada; "
    "vale revisar/atualizar o prompt."
)

for nome in NOMES_FONTES:
    id_bloqueio = f"blq_{proximo_id:03d}"
    nome_escapado = nome.replace("'", "''")

    spark.sql(f"""
        INSERT INTO {TABELA_BLOQUEIOS} (
            id, tipo, nome, fontes_afetadas, descricao, responsavel,
            status_ou_reversibilidade, notas, data_registro
        ) VALUES (
            '{id_bloqueio}', 'descartado', '{nome_escapado}', '{nome_escapado}',
            'Paywall confirmado -- conteúdo bloqueado atrás de assinatura, texto completo não acessível sem login',
            NULL,
            'Sim, se a fonte abrir acesso gratuito ou surgir orçamento para assinatura',
            '{NOTA_COMUM}',
            current_timestamp()
        )
    """)
    print(f"[ok] {id_bloqueio} inserido para '{nome}'")
    proximo_id += 1

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT source_id, nome_fonte, status FROM {TABELA_FONTES} WHERE nome_fonte IN ({placeholders})"))
display(spark.sql(f"SELECT * FROM {TABELA_BLOQUEIOS} WHERE nome IN ({placeholders})"))
