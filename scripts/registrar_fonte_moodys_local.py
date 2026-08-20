# Databricks notebook source
# =============================================================================
# registrar_fonte_moodys_local.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "Moody's" (Moody's Local Brasil -- Ações de Rating), integrada
# ao dispatcher genérico ingest-PDF (ver listar_moodys() /
# CONFIGS_FONTES["moodys_local"]). Já existia linha "Moody's" em
# controle_fontes, mas com source_id="site_page" -- placeholder
# compartilhado herdado do bug de source_id duplicado documentado em
# CLAUDE.md, usado antes por uma abordagem diferente (ingest-news-site-
# inteiro, tratando a home inteira como uma única "página"). UPDATE na
# linha existente em vez de INSERT, por instrução explícita -- NÃO cria
# fonte nova nem duplicata, nome_fonte continua só "Moody's".
#
# Este script documenta uma migração que já rodou fora de um script
# versionado (feita via consulta ad-hoc); mantido no repo pro mesmo
# padrão de trilha de auditoria de scripts/registrar_fonte_cmse_google_news.py.
#
# CUIDADO com `status`, diferente do registro de CMSE: o dispatcher
# genérico ingest-PDF CHAMA atualizar_status_fonte() a cada execução
# real (CMSE não chama). status='Coberta e validada em produção' foi
# setado aqui por instrução explícita do usuário, ANTES de qualquer
# execução real em produção (ultima_execucao ainda nula no momento
# deste registro) -- não é o "Não iniciada" que seria o padrão de Fase
# 3 pra um dispatcher genérico. A partir da primeira execução real, o
# dispatcher passa a gerenciar esse campo sozinho; rodar este script de
# novo depois disso reverteria esse status legítimo -- só rodar de novo
# pra corrigir os outros campos (metodo_captura/notebooks/tasks/notas),
# nunca "só pra garantir idempotência" sem checar o estado atual antes.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"

SOURCE_ID_ANTIGO = "site_page"
SOURCE_ID_NOVO = "moodys_local"

# COMMAND ----------

linha_existente = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES}
    WHERE nome_fonte = "Moody's" AND source_id IN ('{SOURCE_ID_ANTIGO}', '{SOURCE_ID_NOVO}')
""").collect()

if len(linha_existente) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'Moody's' (source_id='{SOURCE_ID_ANTIGO}' ou já migrada), achei {len(linha_existente)}; abortando.")

ja_existe_outro_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES}
    WHERE source_id = '{SOURCE_ID_NOVO}' AND nome_fonte != "Moody's"
""").collect()[0]["qtd"]

if ja_existe_outro_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID_NOVO}' já existe em outra linha de {TABELA_FONTES}; abortando.")

print("[ok] 1 linha 'Moody's' encontrada, nenhum source_id colidindo em outra linha; prosseguindo.")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_fontes
# =============================================================================

metodo_captura = (
    "Download direto de PDF via dispatcher generico ingest-PDF, "
    "listar_moodys() proprio -- roda a mesma logica nas 3 URLs de setor "
    '(Energia/Transporte/Outros em "Projetos de Infraestrutura"), dedup '
    "por URL do item entre elas, navega da listagem ate a pagina de "
    "detalhe de cada rating action pra achar o botao \"Download\" com o "
    "PDF real (link da listagem e pagina HTML, nao PDF direto) -- 403 "
    "intermitente (WAF que alterna), contornado com curl_cffi "
    "(impersonation de TLS, retry ampliado)."
)

notas = (
    "Testado isoladamente em ingestores/GERAL/teste_moodys.ipynb (Fase 1) "
    "-- confirmado 95 itens (Energia 35, Transporte 35, Outros 25) "
    "rodando contra os 3 links de setor reais, 0 duplicados entre "
    "setores, texto extraido via pypdf do PDF real (nao a pagina HTML de "
    "detalhe, que fica so com titulo/data/botao Download). Nota antiga "
    'dizia "403 consistente, precisa modo=navegador" -- nao confirmado '
    "nesta rodada: o 403 e intermitente (WAF que alterna, mesma "
    "categoria de risco da ARTESP), resolvido com retry de curl_cffi "
    "girando perfis de impersonation, sem precisar de navegador real. "
    "Integrado ao dispatcher generico ingest-PDF (antes usava "
    "ingest-news-site-inteiro, tratando a home como uma pagina unica -- "
    'abordagem trocada pra capturar a secao "Ultimas Acoes de Rating" '
    "de verdade). Rodada de teste completa (ate processar_pdf) "
    "confirmada em 2026-08-20."
)

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        source_id = '{SOURCE_ID_NOVO}',
        status = 'Coberta e validada em produção',
        metodo_captura = '{metodo_captura}',
        notebooks_responsaveis = 'ingest-pdfs-generico',
        tasks_responsaveis = 'Ingest-PDF (fonte=moodys_local dentro de todas)',
        notas = '{notas}'
    WHERE nome_fonte = "Moody's"
""")
print(f"[ok] linha 'Moody's' atualizada com source_id='{SOURCE_ID_NOVO}'")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID_NOVO}'"))
