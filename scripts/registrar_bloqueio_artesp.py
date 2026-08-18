# Databricks notebook source
# =============================================================================
# registrar_bloqueio_artesp.py
#
# Registro ÚNICO (roda uma vez). Formaliza "ARTESP" como bloqueada, depois
# do teste isolado de Fase 1 (ver ingestores/TRANSPORTE/teste_artesp.ipynb).
#
# Diagnóstico: dois problemas independentes, empilhados.
#   1. WAF ativo (Imperva/Incapsula) em www.artesp.sp.gov.br -- a mesma URL,
#      com o mesmo User-Agent de navegador, alterna entre devolver o HTML
#      real da página e devolver a página de challenge JS ("Pardon Our
#      Interruption", cookies visid_incap_*/incap_ses_*/nlbi_*), sempre com
#      HTTP 200 (nunca 403/429) -- mesma categoria de risco do bloqueio da
#      ANAC (WAF F5/Shape), vendor diferente (Imperva), comportamento mais
#      intermitente (ANAC bloqueia quase sempre; aqui alterna).
#   2. Quando o WAF deixa passar: a página Sala de Imprensa
#      (/artesp/canais-de-comunicacao/sala-de-imprensa) tem um portlet de
#      listagem quebrado -- exibe aviso nativo do CMS ("Configuração
#      inválida localizada. Entre em contato com o administrador.") em vez
#      da lista de notícias/releases. Bug do lado da ARTESP, independente
#      do WAF.
#   3. /robots.txt não é um robots.txt real -- devolve o mesmo HTML da home
#      institucional, não arquivo de diretivas Disallow/Allow.
#
# Mesmo raciocínio do bloqueio da ANAC: não implementar scraping até
# alguém validar acesso via navegador real dentro do cluster Databricks --
# sem garantia de que resolveria, WAFs desse tipo costumam detectar
# browser headless também, e ainda haveria o portlet quebrado por cima.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_BLOQUEIOS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios"

NOME_FONTE = "ARTESP"

NOTAS_FONTES = (
    "Bloqueada em 18/08/2026, após teste isolado de Fase 1 (ver "
    "ingestores/TRANSPORTE/teste_artesp.ipynb). Dois problemas "
    "independentes, empilhados: (1) WAF ativo (Imperva/Incapsula) em "
    "www.artesp.sp.gov.br -- a mesma URL, com o mesmo User-Agent de "
    "navegador, alterna entre devolver o HTML real da página e devolver a "
    "página de challenge JS (\"Pardon Our Interruption\", cookies "
    "visid_incap_*/incap_ses_*/nlbi_*), sempre com HTTP 200 (nunca "
    "403/429) -- mesma categoria de risco do bloqueio da ANAC (WAF "
    "F5/Shape), vendor diferente, comportamento mais intermitente; (2) "
    "quando o WAF deixa passar, a página Sala de Imprensa tem um portlet "
    "de listagem quebrado -- exibe aviso nativo do CMS (\"Configuração "
    "inválida localizada. Entre em contato com o administrador.\") em vez "
    "da lista de notícias; (3) /robots.txt não é um robots.txt real, "
    "devolve o mesmo HTML da home institucional. Não implementar scraping "
    "até validação com navegador real dentro do cluster Databricks -- sem "
    "garantia de que resolveria (WAF pode detectar headless), e o "
    "problema do portlet quebrado é independente disso."
)

# COMMAND ----------

# =============================================================================
# UPDATE em controle_fontes (placeholder já existe, confirmado)
# =============================================================================

linha_existente = spark.sql(f"""
    SELECT nome_fonte, source_id, status FROM {TABELA_FONTES} WHERE nome_fonte = '{NOME_FONTE}'
""").collect()

if len(linha_existente) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha '{NOME_FONTE}' em {TABELA_FONTES}, achei {len(linha_existente)}; abortando.")

print(f"[info] '{NOME_FONTE}' encontrada (source_id={linha_existente[0]['source_id']!r}, status atual={linha_existente[0]['status']!r}); aplicando UPDATE.")

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        status = 'Bloqueada',
        metodo_captura = 'N/A -- WAF (Imperva/Incapsula) intermitente antes de qualquer captura, e portlet de notícias quebrado no próprio site quando o WAF deixa passar',
        notas = '{NOTAS_FONTES}'
    WHERE nome_fonte = '{NOME_FONTE}'
""")
print(f"[ok] linha '{NOME_FONTE}' atualizada para status='Bloqueada'.")

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
        'WAF ativo (Imperva/Incapsula) em www.artesp.sp.gov.br -- mesma URL alterna entre HTML real e página de challenge JS ("Pardon Our Interruption"), sempre HTTP 200. Quando o WAF deixa passar, a página Sala de Imprensa tem portlet de listagem quebrado (aviso nativo do CMS, "Configuração inválida"). /robots.txt não é robots.txt real (devolve HTML da home).',
        NULL,
        'Talvez, se alguém validar acesso via navegador real (Selenium) dentro do cluster Databricks -- mas sem garantia (WAF pode detectar headless também), e o portlet quebrado no site é um problema independente que precisaria ser corrigido do lado da ARTESP antes de qualquer captura fazer sentido.',
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
