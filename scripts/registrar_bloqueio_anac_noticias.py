# Databricks notebook source
# =============================================================================
# registrar_bloqueio_anac_noticias.py
#
# Registro ÚNICO (roda uma vez). Marca "ANAC" como bloqueada por proteção
# anti-bot ativa (WAF F5/Shape, cookie `TSPD`) no domínio gov.br/anac --
# mesmo uma requisição HTTP isolada recebe página de CAPTCHA em vez do
# conteúdo (HTTP 200), e repetição escala para 429 e depois bloqueio
# temporário do IP de origem. Não é `robots.txt` (que não lista /anac),
# então diferente do caso AGENERSA em causa, mas mesmo efeito prático:
# status='Bloqueada' em controle_fontes, igual reservado para bloqueio
# técnico (ver descartar_brasil_energia_canal_energia.py).
#
# Se existir placeholder "ANAC" em controle_fontes (catálogo original,
# mesmo padrão de ANATEL/ANP/ANA/ABEGÁS), faz UPDATE nele. Caso contrário,
# faz INSERT de uma linha nova (nunca esteve no catálogo original).
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_BLOQUEIOS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios"

NOME_FONTE = "ANAC"

NOTAS_FONTES = (
    "Bloqueada por WAF anti-bot (F5/Shape, cookie TSPD) em gov.br/anac -- "
    "requisição HTTP isolada já recebe CAPTCHA (200), repetição escala para "
    "429 e bloqueio temporário de IP. Página de notícias identificada "
    "(gov.br/anac/pt-br/noticias/ultimas-noticias-2) e possível RSS "
    "(gov.br/anac/RSS), nenhum dos dois validado -- caem atrás do mesmo "
    "bloqueio. Diferente do bloqueio de Selenium (Brazil Journal/PPI): "
    "sem garantia de que um browser automatizado passe por essa proteção, "
    "esse tipo de WAF costuma detectar headless também. Não implementar "
    "scraping até validação com navegador real dentro do cluster Databricks."
)

# COMMAND ----------

# =============================================================================
# UPDATE (placeholder existente) ou INSERT (linha nova) em controle_fontes
# =============================================================================

linha_existente = spark.sql(f"""
    SELECT nome_fonte, source_id, status FROM {TABELA_FONTES} WHERE nome_fonte = '{NOME_FONTE}'
""").collect()

if len(linha_existente) > 1:
    raise RuntimeError(f"Esperava no máximo 1 linha '{NOME_FONTE}' em {TABELA_FONTES}, achei {len(linha_existente)}; abortando.")

if len(linha_existente) == 1:
    print(f"[info] placeholder '{NOME_FONTE}' já existe (source_id={linha_existente[0]['source_id']!r}, status={linha_existente[0]['status']!r}); aplicando UPDATE.")
    spark.sql(f"""
        UPDATE {TABELA_FONTES}
        SET
            status = 'Bloqueada',
            metodo_captura = 'N/A -- bloqueado por WAF anti-bot antes de qualquer tentativa de captura',
            notas = '{NOTAS_FONTES}'
        WHERE nome_fonte = '{NOME_FONTE}'
    """)
    print(f"[ok] linha '{NOME_FONTE}' atualizada para status='Bloqueada'.")
else:
    print(f"[info] nenhum placeholder '{NOME_FONTE}' encontrado; inserindo linha nova.")
    spark.sql(f"""
        INSERT INTO {TABELA_FONTES} (
            source_id, nome_fonte, tipo_categoria, setor, importancia_original,
            status, granularidade_conteudo, confidence_tier, metodo_captura,
            notebooks_responsaveis, tasks_responsaveis, cadencia_real, notas
        ) VALUES (
            '—', '{NOME_FONTE}', 'Agências Reguladoras Federais', 'Transporte',
            NULL, 'Bloqueada', NULL, NULL,
            'N/A -- bloqueado por WAF anti-bot antes de qualquer tentativa de captura',
            NULL, NULL, NULL,
            '{NOTAS_FONTES}'
        )
    """)
    print(f"[ok] linha nova '{NOME_FONTE}' inserida com status='Bloqueada'.")

# COMMAND ----------

# =============================================================================
# INSERT em controle_bloqueios (trava de segurança: aborta se já existir)
# =============================================================================

ja_existe_bloqueio = spark.sql(f"""
    SELECT id FROM {TABELA_BLOQUEIOS} WHERE nome = '{NOME_FONTE}'
""").collect()

if ja_existe_bloqueio:
    raise RuntimeError(f"Já existe bloqueio registrado para '{NOME_FONTE}' (id={ja_existe_bloqueio[0]['id']}); abortando.")

proximo_id = spark.sql(f"""
    SELECT MAX(CAST(SUBSTRING(id, 5) AS INT)) AS max_num FROM {TABELA_BLOQUEIOS}
    WHERE id LIKE 'blq_%'
""").collect()[0]["max_num"] + 1

id_bloqueio = f"blq_{proximo_id:03d}"

spark.sql(f"""
    INSERT INTO {TABELA_BLOQUEIOS} (
        id, tipo, nome, fontes_afetadas, descricao, responsavel,
        status_ou_reversibilidade, notas, data_registro
    ) VALUES (
        '{id_bloqueio}', 'bloqueado', '{NOME_FONTE}', '{NOME_FONTE}',
        'Proteção anti-bot ativa (WAF F5/Shape, cookie TSPD) em gov.br/anac -- requisição HTTP isolada já recebe CAPTCHA (200) em vez do conteúdo real; não é robots.txt (não lista /anac). Repetição escala para HTTP 429 e depois bloqueio temporário de IP.',
        NULL,
        'Sim, se alguém validar acesso via navegador real (Selenium) dentro do cluster Databricks -- mas sem garantia, WAFs desse tipo costumam detectar browser headless também. Depende do mesmo desbloqueio de Chrome/init script já pendente para Brazil Journal/PPI, mais essa validação extra.',
        '{NOTAS_FONTES}',
        current_timestamp()
    )
""")
print(f"[ok] {id_bloqueio} inserido para '{NOME_FONTE}'")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT source_id, nome_fonte, status, notas FROM {TABELA_FONTES} WHERE nome_fonte = '{NOME_FONTE}'"))
display(spark.sql(f"SELECT * FROM {TABELA_BLOQUEIOS} WHERE nome = '{NOME_FONTE}'"))
