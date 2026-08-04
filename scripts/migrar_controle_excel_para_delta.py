# Databricks notebook source
# =============================================================================
# migrar_controle_excel_para_delta.py
#
# Migração ÚNICA (roda uma vez) da planilha de controle
# (kinea_infra_controle_fontes.xlsx) para tabelas Delta no Unity Catalog.
#
# A partir do momento em que essa migração rodar com sucesso, o Excel deixa
# de ser a fonte de verdade — as tabelas abaixo passam a ser atualizadas
# diretamente pelo pipeline (status, execuções) e via Claude Code / SQL
# (campos manuais como observações e prioridade).
#
# Segurança: o script recusa rodar se as tabelas já existirem, a menos que
# FORCAR_SOBRESCRITA = True — isso evita apagar dados que o pipeline já
# tenha escrito depois da primeira migração.
# =============================================================================

# COMMAND ----------

# =============================================================================
# Dependência: openpyxl (necessária para ler .xlsx via pandas).
# Não vem instalada por padrão no cluster — instalamos aqui para que o
# notebook funcione de forma reprodutível, sem precisar de configuração
# manual no cluster.
# =============================================================================

# MAGIC %pip install openpyxl

# COMMAND ----------

# Reinicia o interpretador Python para que o pacote recém-instalado seja
# reconhecido (necessário no Databricks após %pip install).
dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# Configuração
# =============================================================================

# Reaproveitamos o schema "research", que já existe e onde o usuário já tem
# permissão de escrita comprovada (é onde o pipeline de ingestão grava hoje).
# Criar um schema novo ("controle") exigiria privilégio CREATE SCHEMA no
# catálogo, que esse usuário não tem — em vez disso, diferenciamos as tabelas
# de controle por um prefixo (controle_...).
CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

# Caminho do Excel atual. Ajuste para onde você subir o arquivo
# (ex.: dentro de um Volume, ou anexado ao workspace).
CAMINHO_EXCEL = "/Volumes/desafio_kinea/research/research_volume/controle/kinea_infra_controle_fontes.xlsx"

# Mude para True apenas se tiver certeza de que quer sobrescrever tabelas
# já existentes (isso apaga qualquer atualização feita pelo pipeline desde
# a última migração).
FORCAR_SOBRESCRITA = False

# COMMAND ----------

# =============================================================================
# Dependências
# =============================================================================

import pandas as pd
from datetime import datetime, timezone

# Não criamos catálogo nem schema aqui de propósito — ambos já existem e
# criação exige privilégios de admin que este usuário não tem. Se algum dia
# isso mudar (schema "controle" dedicado, criado por um admin), basta voltar
# SCHEMA = "controle" e PREFIXO = "" e essa migração passa a valer sem
# nenhuma outra alteração no restante do script.

TABELAS_ALVO = [
    f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes",
    f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks",
    f"{CATALOGO}.{SCHEMA}.{PREFIXO}tasks",
    f"{CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios",
    f"{CATALOGO}.{SCHEMA}.{PREFIXO}changelog_historico",
]

if not FORCAR_SOBRESCRITA:
    existentes = [t for t in TABELAS_ALVO if spark.catalog.tableExists(t)]
    if existentes:
        raise RuntimeError(
            "As seguintes tabelas já existem e FORCAR_SOBRESCRITA=False, "
            f"abortando para não perder dados: {existentes}"
        )

# COMMAND ----------

# =============================================================================
# Leitura do Excel (todas as abas relevantes)
# =============================================================================

xls = pd.ExcelFile(CAMINHO_EXCEL)

df_fontes_raw = xls.parse("Fontes")
df_notebooks_raw = xls.parse("Notebooks")
df_tasks_raw = xls.parse("Tasks")
df_bloqueios_raw = xls.parse("Bloqueios")
df_descartadas_raw = xls.parse("Descartadas")
df_changelog_raw = xls.parse("Changelog")

print(f"Fontes: {len(df_fontes_raw)} linhas")
print(f"Notebooks: {len(df_notebooks_raw)} linhas")
print(f"Tasks: {len(df_tasks_raw)} linhas")
print(f"Bloqueios: {len(df_bloqueios_raw)} linhas")
print(f"Descartadas: {len(df_descartadas_raw)} linhas")
print(f"Changelog: {len(df_changelog_raw)} linhas")

# COMMAND ----------

# =============================================================================
# 1) fontes
# =============================================================================
# Colunas novas (ultima_execucao, docs_capturados, ultimo_erro) começam
# vazias — o pipeline passa a preenchê-las a cada execução, dali pra frente.

df_fontes = pd.DataFrame({
    "source_id": df_fontes_raw["source_id (schema)"],
    "nome_fonte": df_fontes_raw["Fonte"],
    "tipo_categoria": df_fontes_raw["Tipo (categoria original)"],
    "setor": df_fontes_raw["Setor"],
    "importancia_original": df_fontes_raw["Importância (original)"],
    "status": df_fontes_raw["Status"],
    "granularidade_conteudo": df_fontes_raw["Granularidade do conteúdo"],
    "confidence_tier": df_fontes_raw["Confidence tier"],
    "metodo_captura": df_fontes_raw["Método de captura"],
    "notebooks_responsaveis": df_fontes_raw["Notebook(s) responsável(is)"],
    "tasks_responsaveis": df_fontes_raw["Task(s) responsável(is)"],
    "cadencia_real": df_fontes_raw["Cadência real observada"],
    "notas": df_fontes_raw["Notas / dificuldades"],
})
df_fontes["ultima_execucao"] = pd.NaT
df_fontes["docs_capturados"] = pd.array([None] * len(df_fontes), dtype="Int64")
df_fontes["ultimo_erro"] = None
df_fontes["data_migracao"] = datetime.now(timezone.utc)
# Nota: essas 3 primeiras colunas nascem 100% vazias (nenhum pipeline rodou
# ainda), então o Spark não consegue inferir o tipo delas sozinho a partir
# do pandas (viram NullType, o que quebra escritas futuras). O tipo correto
# é forçado explicitamente logo abaixo, na função `escrever`.

# Linhas sem source_id não têm como virar chave — ficam de fora e são
# reportadas, em vez de entrar silenciosamente com chave nula.
sem_id = df_fontes[df_fontes["source_id"].isna()]
if len(sem_id) > 0:
    print(f"[aviso] {len(sem_id)} fonte(s) sem source_id preenchido, não migradas:")
    print(sem_id["nome_fonte"].tolist())
df_fontes = df_fontes[df_fontes["source_id"].notna()].reset_index(drop=True)

# COMMAND ----------

# =============================================================================
# 2) notebooks
# =============================================================================

df_notebooks = pd.DataFrame({
    "notebook_nome": df_notebooks_raw["Notebook"],
    "caminho_repo": df_notebooks_raw["Caminho no repo (Git)"],
    "padrao_arquitetura": df_notebooks_raw["Padrão de arquitetura"],
    "parametrizado": df_notebooks_raw["Parametrizado?"],
    "fontes_cobertas": df_notebooks_raw["Fontes cobertas"],
    "dependencias_externas": df_notebooks_raw["Dependências externas"],
    "notas": df_notebooks_raw["Notas"],
})

# COMMAND ----------

# =============================================================================
# 3) tasks
# =============================================================================

df_tasks = pd.DataFrame({
    "job": df_tasks_raw["Job"],
    "nome_task": df_tasks_raw["Nome da task"],
    "notebook": df_tasks_raw["Notebook"],
    "parametro_fonte": df_tasks_raw["Parâmetro 'fonte'"],
    "modo_execucao": df_tasks_raw["Modo de execução"],
    "cluster": df_tasks_raw["Cluster"],
    "depends_on": df_tasks_raw["Depends on"],
    "horario_agendado": df_tasks_raw["Horário agendado"].astype(str),
})

# COMMAND ----------

# =============================================================================
# 4) bloqueios  (une Bloqueios + Descartadas, diferenciadas por `tipo`)
# =============================================================================

df_bloqueios_parte = pd.DataFrame({
    "tipo": "bloqueado",
    "nome": df_bloqueios_raw["Bloqueio"],
    "fontes_afetadas": df_bloqueios_raw["Fontes afetadas"],
    "descricao": df_bloqueios_raw["Descrição"],
    "responsavel": df_bloqueios_raw["Responsável por resolver"],
    "status_ou_reversibilidade": df_bloqueios_raw["Status"],
    "notas": df_bloqueios_raw["Notas"],
})

df_descartadas_parte = pd.DataFrame({
    "tipo": "descartado",
    "nome": df_descartadas_raw["Fonte"],
    "fontes_afetadas": df_descartadas_raw["Fonte"],
    "descricao": df_descartadas_raw["Motivo do descarte"],
    "responsavel": None,
    "status_ou_reversibilidade": df_descartadas_raw["Reversível?"],
    "notas": df_descartadas_raw["Notas"],
})

df_bloqueios = pd.concat([df_bloqueios_parte, df_descartadas_parte], ignore_index=True)
df_bloqueios.insert(0, "id", [f"blq_{i:03d}" for i in range(1, len(df_bloqueios) + 1)])
df_bloqueios["data_registro"] = datetime.now(timezone.utc)

# COMMAND ----------

# =============================================================================
# 5) changelog_historico  (snapshot congelado — não recebe writes depois;
#    o registro de mudanças futuras vem do DESCRIBE HISTORY das tabelas acima)
# =============================================================================

df_changelog = pd.DataFrame({
    "data_evento": pd.to_datetime(df_changelog_raw["Data"]),
    "problema": df_changelog_raw["Problema"],
    "causa_raiz": df_changelog_raw["Causa raiz"],
    "correcao": df_changelog_raw["Correção"],
    "notebooks_afetados": df_changelog_raw["Notebook(s) afetado(s)"],
})

# COMMAND ----------

# =============================================================================
# Escrita das tabelas Delta
# =============================================================================

from pyspark.sql import types as T

def escrever(df_pandas: pd.DataFrame, nome_tabela: str, tipos_explicitos: dict = None):
    df_spark = spark.createDataFrame(df_pandas)
    # Força o tipo de colunas que podem ter nascido 100% vazias, já que o
    # Spark não consegue inferir tipo a partir de puro nulo (viraria
    # NullType, que quebra escritas/leituras futuras).
    if tipos_explicitos:
        for coluna, tipo in tipos_explicitos.items():
            df_spark = df_spark.withColumn(coluna, df_spark[coluna].cast(tipo))
    (
        df_spark
        .write.format("delta")
        .mode("overwrite" if FORCAR_SOBRESCRITA else "error")
        .saveAsTable(nome_tabela)
    )
    print(f"[ok] {nome_tabela}: {len(df_pandas)} linhas gravadas")

escrever(
    df_fontes,
    f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes",
    tipos_explicitos={
        "ultima_execucao": T.TimestampType(),
        "docs_capturados": T.IntegerType(),
        "ultimo_erro": T.StringType(),
    },
)
escrever(df_notebooks, f"{CATALOGO}.{SCHEMA}.{PREFIXO}notebooks")
escrever(df_tasks, f"{CATALOGO}.{SCHEMA}.{PREFIXO}tasks")
escrever(df_bloqueios, f"{CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios")
escrever(df_changelog, f"{CATALOGO}.{SCHEMA}.{PREFIXO}changelog_historico")

# COMMAND ----------

# =============================================================================
# Chaves primárias (informativas — Unity Catalog não bloqueia duplicatas
# por padrão, mas isso documenta a chave e ajuda ferramentas/BI a entender
# a modelagem)
# =============================================================================

spark.sql(f"ALTER TABLE {CATALOGO}.{SCHEMA}.{PREFIXO}fontes ALTER COLUMN source_id SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOGO}.{SCHEMA}.{PREFIXO}fontes ADD CONSTRAINT pk_{PREFIXO}fontes PRIMARY KEY (source_id)")

spark.sql(f"ALTER TABLE {CATALOGO}.{SCHEMA}.{PREFIXO}notebooks ALTER COLUMN notebook_nome SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOGO}.{SCHEMA}.{PREFIXO}notebooks ADD CONSTRAINT pk_{PREFIXO}notebooks PRIMARY KEY (notebook_nome)")

spark.sql(f"ALTER TABLE {CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios ALTER COLUMN id SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios ADD CONSTRAINT pk_{PREFIXO}bloqueios PRIMARY KEY (id)")

# COMMAND ----------

# =============================================================================
# Verificação rápida
# =============================================================================

for t in TABELAS_ALVO:
    n = spark.table(t).count()
    print(f"{t}: {n} linhas")

display(spark.table(f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes").limit(10))
