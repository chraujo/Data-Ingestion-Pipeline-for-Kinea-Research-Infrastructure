# Databricks notebook source
# =============================================================================
# registrar_bloqueio_agrese_saneamento.py
#
# Registro ÚNICO (roda uma vez). Formaliza "AGRESE" como bloqueada, depois
# de uma segunda investigação (18/08/2026, retestando a instabilidade
# anotada em 14/08/2026 -- ver registrar_investigacao_agrese_saneamento.py
# e MENSAGENS.md, seção Bloqueios).
#
# Diagnóstico mais completo desta rodada:
#   - robots.txt permite tudo (Disallow vazio)
#   - Homepage (agrese.se.gov.br/) e páginas estáticas (ex.: /institucional/,
#     /portarias/) respondem HTTP 200 normalmente
#   - Feed RSS (agrese.se.gov.br/feed/) responde HTTP 200 e lista itens, mas
#     o mais recente é de 30/06/2026 -- quase 7 semanas sem conteúdo novo
#   - URLs de POSTS de notícia (o que o feed lista) devolvem HTTP 301 para
#     https://www.se.gov.br/agencia (página genérica, sem conteúdo) -- testado
#     com e sem "www.", sem barra final, e via permalink "feio" (?p=ID);
#     todos convergem pro mesmo redirecionamento quebrado
#   - API REST do WordPress (/wp-json/wp/v2/posts) devolve HTTP 401
#     ("rest_not_logged_in") -- trancada, não dá pra contornar por aí
#   - O link "AGRESE" no diretório de órgãos do novo portal central
#     (se.gov.br/agencia/orgaos_entidades) só redireciona de volta pro
#     mesmo domínio antigo (agrese.se.gov.br/), não leva a um lugar novo
#
# Padrão (só posts quebrados, páginas estáticas OK) tem cara de bug de
# redirecionamento canônico no WordPress -- não de bloqueio deliberado
# (sem WAF, sem robots.txt restritivo, sem CAPTCHA). Por isso o campo
# `status_ou_reversibilidade` abaixo documenta que isso é, em princípio,
# reversível a qualquer momento se o problema for corrigido do lado do
# governo de Sergipe -- diferente de bloqueios por WAF (ANAC) ou
# robots.txt (AGENERSA), que exigem uma mudança de política pra reverter.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_BLOQUEIOS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios"

NOME_FONTE = "AGRESE"

NOTAS_FONTES = (
    "Bloqueada em 18/08/2026, após segunda investigação (primeira em "
    "14/08/2026, ver histórico em notas anteriores desta linha e em "
    "MENSAGENS.md). robots.txt permite tudo; homepage e páginas estáticas "
    "(/institucional/, /portarias/) respondem 200 normalmente; feed RSS "
    "(agrese.se.gov.br/feed/) responde 200 e lista itens, mas o mais "
    "recente é de 30/06/2026 -- quase 7 semanas sem conteúdo novo. URLs de "
    "POSTS de notícia (o que o feed lista) devolvem 301 para "
    "https://www.se.gov.br/agencia (página genérica) -- testado com/sem "
    "www., sem barra final, e via permalink '?p=ID', todos convergem pro "
    "mesmo redirecionamento quebrado. API REST do WordPress "
    "(/wp-json/wp/v2/posts) devolve 401, trancada. Padrão (só posts "
    "quebrados, páginas OK) sugere bug de redirecionamento canônico no "
    "WordPress do governo de Sergipe, não bloqueio deliberado -- sem WAF, "
    "sem robots.txt restritivo, sem CAPTCHA. Por isso classificado como "
    "potencialmente reversível; recomendado retestar periodicamente "
    "(ver controle_bloqueios.status_ou_reversibilidade)."
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
        metodo_captura = 'N/A -- URLs de post (notícia) redirecionam (301) para página genérica; feed RSS lista itens mas sem acesso ao texto completo',
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
        'URLs de posts de notícia em agrese.se.gov.br devolvem HTTP 301 para uma página genérica (https://www.se.gov.br/agencia), sem conteúdo real -- páginas estáticas do mesmo site funcionam normalmente (200). Feed RSS lista itens mas sem link utilizável para o texto completo; API REST (wp-json) trancada (401).',
        NULL,
        'Provavelmente reversível -- padrão (só posts quebrados, páginas OK) sugere bug de redirecionamento canônico no WordPress do governo de Sergipe, não bloqueio deliberado (sem WAF, sem robots.txt restritivo, sem CAPTCHA). Recomendado retestar em algumas semanas; se voltar a funcionar, encaixa direto no dispatcher genérico de RSS (ingest-news-rss-infra), mesmo padrão da ARISB-MG.',
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
