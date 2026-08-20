# Databricks notebook source
# =============================================================================
# registrar_fonte_abcr.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# para "ABCR" (Associação Brasileira de Concessionárias de Rodovias),
# integrada ao dispatcher genérico ingest-scraping (ver listar_abcr() /
# CONFIGS_FONTES["abcr"], commit "Add ABCR to ingest-scraping generic
# dispatcher (Fase 2)"). Já existia linha placeholder "ABCR" em
# controle_fontes (source_id="—"). UPDATE na linha existente em vez de
# INSERT, mesmo padrão de CMSE/ONS/ANP/... -- NÃO cria fonte nova com
# outro nome (nome_fonte continua só "ABCR", sem "Notícias" em nenhum
# lugar, por instrução explícita).
#
# Este script documenta uma migração que já rodou fora de um script
# versionado (feita via consulta ad-hoc); mantido no repo pro mesmo
# padrão de trilha de auditoria de scripts/registrar_fonte_cmse_google_news.py
# -- rodar de novo é seguro (idempotente, os valores já batem com o que
# está na tabela).
#
# IMPORTANTE, diferente do registro de CMSE: o dispatcher genérico
# ingest-scraping CHAMA atualizar_status_fonte() a cada execução real
# (CMSE não chama) -- por isso este script NÃO seta a coluna `status`
# no UPDATE. Deixar `status` de fora garante que rodar este script de
# novo no futuro (ex.: pra corrigir algum outro campo) nunca sobrescreve
# o status real gerenciado pelo dispatcher a cada execução em produção.
# O status inicial da Fase 3 foi "Não iniciada"; em 2026-08-20 já
# aparecia "Coberta e validada em produção" após a primeira execução
# real (ultima_execucao confirmada, docs_capturados=0 -- nenhum item
# novo desde então, esperado dado o histórico pequeno da fonte).
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"

SOURCE_ID = "abcr"

# COMMAND ----------

linha_existente = spark.sql(f"""
    SELECT nome_fonte, source_id FROM {TABELA_FONTES} WHERE nome_fonte = 'ABCR'
""").collect()

if len(linha_existente) != 1:
    raise RuntimeError(f"Esperava exatamente 1 linha 'ABCR' em {TABELA_FONTES}, achei {len(linha_existente)}; abortando.")

ja_existe_outro_source_id = spark.sql(f"""
    SELECT COUNT(*) AS qtd FROM {TABELA_FONTES}
    WHERE source_id = '{SOURCE_ID}' AND nome_fonte != 'ABCR'
""").collect()[0]["qtd"]

if ja_existe_outro_source_id > 0:
    raise RuntimeError(f"source_id='{SOURCE_ID}' já existe em outra linha de {TABELA_FONTES}; abortando.")

print("[ok] 1 linha 'ABCR' encontrada, nenhum source_id colidindo em outra linha; prosseguindo.")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_fontes
# =============================================================================

spark.sql(f"""
    UPDATE {TABELA_FONTES}
    SET
        source_id = '{SOURCE_ID}',
        tipo_categoria = 'Associações Setoriais (Rodovias)',
        setor = 'Transporte',
        importancia_original = 'Baixa',
        metodo_captura = 'Scraping HTML renderizado no servidor (WordPress + Elementor Essential Addons) via dispatcher generico ingest-scraping, sem paginacao (AJAX-only, primeiro lote apenas)',
        notebooks_responsaveis = 'ingest-scraping-generico',
        tasks_responsaveis = 'Ingest-scraping',
        notas = 'Testado isoladamente em ingestores/TRANSPORTE/teste_abcr.ipynb (Fase 1). robots.txt tem bloco Cloudflare que nomeia varios bots de IA (ClaudeBot, GPTBot, Google-Extended etc.) com Disallow: / mas o grupo generico User-agent: * permite tudo -- usuario consultado e autorizou explicitamente prosseguir antes de qualquer requisicao. Listagem em article.eael-grid-post (widget Elementor eael-grid-post), titulo/link/resumo prontos no HTML estatico. Paginacao real e AJAX-only (botao Carregar mais, admin-ajax.php) -- /noticias/page/N/ NAO e paginacao real (mesmo conteudo da pagina 1, confirmado em Fase 1) -- captura limitada ao primeiro lote (4 itens por execucao), aceito por instrucao explicita. Data de publicacao so na pagina individual, primeiro <time> no formato MES DIA, ANO (ex. agosto 12, 2026). Texto completo em .elementor-widget-theme-post-content, sem paywall, sem WAF.'
    WHERE nome_fonte = 'ABCR'
""")
print(f"[ok] linha 'ABCR' atualizada com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'"))
