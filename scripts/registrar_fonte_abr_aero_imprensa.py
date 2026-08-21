# Databricks notebook source
# =============================================================================
# registrar_fonte_abr_aero_imprensa.py
#
# Registro ÚNICO (roda uma vez). Fase 3 do fluxo de adição de fonte nova
# (ver CLAUDE.md) para "ABR - Aeroportos do Brasil - Assessoria de
# Imprensa". Não existia nenhuma linha "ABR" em controle_fontes (nem como
# placeholder do catálogo original de 79 fontes) -- INSERT de linha nova,
# não UPDATE. Confirmado antes de rodar: 0 linhas com nome_fonte/source_id
# contendo "abr" isolado (existe "abar_noticias"/"ABAR" e
# "abrace_noticias"/"ABRACE" na tabela -- são fontes diferentes, checagem
# abaixo usa palavra isolada pra não confundir com essas).
#
# Importância definida diretamente ao registrar (mesmo critério de ABAR):
#   "Média" -- associação nacional que representa 59 aeroportos federais
#   concedidos (>93% do tráfego de passageiros do país), mas o conteúdo da
#   página de imprensa é 100% institucional (releases da própria ABR:
#   eventos, campanhas, estudos, posicionamento regulatório) -- não é uma
#   agência reguladora com decisão pontual (que justificaria "Alta"), nem
#   uma associação de nicho de baixo volume (que justificaria "Baixa").
# =============================================================================

# COMMAND ----------

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

TABELA_FONTES = f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes"
TABELA_NOTEBOOKS = f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks"

SOURCE_ID = "abr_aero_imprensa"
NOME_FONTE = "ABR"
NOTEBOOK_NOME = "ingest-scraping-generico"

# COMMAND ----------

linhas_existentes = spark.sql(f"""
    SELECT source_id, nome_fonte FROM {TABELA_FONTES}
    WHERE nome_fonte = 'ABR' OR source_id = '{SOURCE_ID}'
""").collect()

if linhas_existentes:
    raise RuntimeError(f"Já existe linha 'ABR'/'{SOURCE_ID}' em {TABELA_FONTES}: {linhas_existentes}; abortando (esperava INSERT novo, não UPDATE).")

print("[ok] nenhuma linha 'ABR' pré-existente, nenhum source_id duplicado; prosseguindo com INSERT.")

# COMMAND ----------

linha_notebook = spark.sql(f"""
    SELECT notebook_nome, fontes_cobertas FROM {TABELA_NOTEBOOKS}
    WHERE notebook_nome = '{NOTEBOOK_NOME}'
""").collect()

if not linha_notebook:
    raise RuntimeError(f"notebook_nome='{NOTEBOOK_NOME}' não encontrado em {TABELA_NOTEBOOKS}; esperado já existir.")

fontes_atuais = linha_notebook[0]["fontes_cobertas"] or ""
print(f"[info] fontes_cobertas atual de '{NOTEBOOK_NOME}': {fontes_atuais!r}")

# COMMAND ----------

# =============================================================================
# INSERT em controle_fontes -- linha nova (sem placeholder pré-existente)
# =============================================================================

spark.sql(f"""
    INSERT INTO {TABELA_FONTES} (
        source_id, nome_fonte, tipo_categoria, setor, importancia_original,
        status, granularidade_conteudo, confidence_tier, metodo_captura,
        notebooks_responsaveis, tasks_responsaveis, cadencia_real, notas,
        ultima_execucao, docs_capturados, ultimo_erro, data_migracao,
        coberta_por, grupo_exibicao
    ) VALUES (
        '{SOURCE_ID}',
        '{NOME_FONTE}',
        'Associação Setorial (Aviação/Aeroportos)',
        'Transporte',
        'Média',
        'Não iniciada',
        'N/D',
        'N/D',
        'Scraping HTML renderizado no servidor (WordPress + Elementor, plugin tf-post) via dispatcher generico ingest-scraping, sem paginacao (pagina unica)',
        '{NOTEBOOK_NOME}',
        'Ingest-scraping',
        'N/D',
        'Associacao que representa os 59 aeroportos federais concedidos (>93% do trafego de passageiros do pais). Pagina de imprensa (https://abr.aero/pt/imprensa/) tem historico pequeno e sem paginacao (5 releases, mar/2024 a dez/2025) -- sem necessidade de max_paginas. Data na listagem vem em formato americano MM/DD/YYYY (li.post-date a), apesar do site ser pt-BR -- confirmado comparando com a URL do post (/pt/AAAA/MM/DD/). Conteudo 100% institucional (releases da propria ABR) -- sem filtro de relevancia na captura (proposital, fica para a etapa de NLP). Testado isoladamente em ingestores/TRANSPORTE/teste_abr.ipynb (Fase 1).',
        NULL,
        0,
        NULL,
        NULL,
        NULL,
        NULL
    )
""")
print(f"[ok] linha 'ABR' inserida com source_id='{SOURCE_ID}'")

# COMMAND ----------

# =============================================================================
# UPDATE em controle_notebooks -- acrescenta abr_aero_imprensa em fontes_cobertas
# =============================================================================

fontes_lista = [f.strip() for f in fontes_atuais.split(",") if f.strip()]
if SOURCE_ID not in fontes_lista:
    fontes_lista.append(SOURCE_ID)
novo_valor = ", ".join(fontes_lista)

spark.sql(f"""
    UPDATE {TABELA_NOTEBOOKS}
    SET fontes_cobertas = '{novo_valor}'
    WHERE notebook_nome = '{NOTEBOOK_NOME}'
""")
print(f"[ok] fontes_cobertas de '{NOTEBOOK_NOME}' atualizado para: {novo_valor!r}")

# COMMAND ----------

# =============================================================================
# Verificação
# =============================================================================

display(spark.sql(f"SELECT * FROM {TABELA_FONTES} WHERE source_id = '{SOURCE_ID}'"))
display(spark.sql(f"SELECT notebook_nome, fontes_cobertas FROM {TABELA_NOTEBOOKS} WHERE notebook_nome = '{NOTEBOOK_NOME}'"))
