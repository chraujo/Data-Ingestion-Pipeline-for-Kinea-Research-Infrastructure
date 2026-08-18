# Databricks notebook source
# =============================================================================
# registrar_investigacao_artemig_transporte.py
#
# Registro ÚNICO (roda uma vez). Atualiza o placeholder "ARTEMIG" (catálogo
# original, já existia em controle_fontes com status='Não iniciada') para
# refletir a investigação de Fase 1 (ver
# ingestores/TRANSPORTE/teste_artemig.ipynb) -- site WordPress comum, sem
# WAF, robots.txt permite scraping, MAS não há nenhuma notícia publicada
# no momento: widget "Últimas notícias" da home vazio, categoria
# /category/noticias/ (linkada pela própria home) devolve "Nenhum post
# encontrado", e o único fluxo de publicação ativo (post do WordPress,
# atualizado até 13/08/2026) só é usado para páginas institucionais
# (Plano de Comunicação, Editais, trechos de rodovia, etc.), não para
# notícias/eventos datados.
#
# Diferente de um bloqueio técnico (WAF/robots.txt) -- aqui é ausência de
# conteúdo do lado da fonte, não um obstáculo nosso. Site claramente segue
# ativo (páginas atualizadas até junho/2026), então não é definitivo --
# UPDATE, não INSERT -- preserva tipo_categoria, setor e
# importancia_original já presentes no placeholder original. source_id
# permanece "—": sem conteúdo real pra validar, não faz sentido codificar
# um listar_artemig() ainda.
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"

NOME_FONTE = "ARTEMIG"
NOVO_STATUS = "Iniciada — não validada"

NOTAS = (
    "Investigação de Fase 1 em 18/08/2026 (ver "
    "ingestores/TRANSPORTE/teste_artemig.ipynb): site WordPress comum "
    "(Elementor), sem WAF, robots.txt permite scraping de conteúdo -- "
    "tecnicamente encaixaria no dispatcher generico ingest-scraping sem "
    "obstaculo tecnico. MAS nao ha nenhuma noticia publicada no momento: "
    "widget \"Ultimas noticias\" da home vazio, categoria "
    "/category/noticias/ (linkada pela propria home, botao \"Ver todas as "
    "noticias\") devolve \"Nenhum post encontrado\", e o unico fluxo de "
    "publicacao ativo (post do WordPress, post-sitemap.xml atualizado ate "
    "13/08/2026) e usado so para paginas institucionais (Plano de "
    "Comunicacao, Editais, trechos de rodovia concedida, etc.), sem "
    "nenhum evento/decisao/anuncio datado -- confirmado nas duas "
    "primeiras paginas de /posts/ (20 itens, ago/2025 a jun/2026), nenhum "
    "e noticia. Diferente de bloqueio tecnico (WAF/robots.txt): e "
    "ausencia de conteudo do lado da fonte, nao obstaculo nosso. Site "
    "segue ativo (paginas atualizadas ate junho/2026) -- vale retestar "
    "periodicamente; se a ARTEMIG voltar a publicar noticias reais, "
    "integracao e direta (WordPress puro, mesmo padrao de Acende "
    "Brasil/ABEGAS/AESBE). Nota operacional: certificado TLS incompleto "
    "no servidor (falta intermediario) -- testar sem verify=False "
    "primeiro, certifi as vezes ja cobre."
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
