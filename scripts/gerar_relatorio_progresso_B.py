# Databricks notebook source
# =============================================================================
# gerar_relatorio_progresso.py
#
# Relatório de progresso do projeto, em formato de seções empilhadas dentro
# de um único arquivo HTML. Lê das tabelas de controle + do arquivo
# MENSAGENS.md (no repo) para anotações manuais por seção.
#
# IMPORTANTE: rode scripts/corrigir_dados_relatorio.py pelo menos uma vez
# antes deste notebook — ele cria as colunas `coberta_por` e
# `grupo_exibicao` que este relatório espera encontrar.
#
# Seções do relatório:
#   1. Fontes             — fichas compactas expansíveis, por importância
#                            (Alta/Média/Baixa), cobertas primeiro
#   2. Bloqueios           — bloqueios e fontes descartadas
#   3. Últimas atualizações
#
# Cada seção tem, no rodapé, um espaço de anotação livre (texto de
# MENSAGENS.md daquela seção específica).
#
# Publica também uma cópia no Azure Blob Storage com link temporário (SAS
# token, 30 dias) -- as fichas de fonte usam <details>/<summary> nativo do
# HTML, que funciona bem em navegador/Gmail/Apple Mail mas não em Outlook
# Desktop (que usa o motor do Word); o link dá a quem recebe o e-mail uma
# forma garantida de abrir a versão interativa de verdade, fora do cliente
# de e-mail.
# =============================================================================

# COMMAND ----------

# MAGIC %pip install --quiet azure-storage-blob
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import json
import pandas as pd
from datetime import datetime, timezone

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

RAIZ_REPO = "/Workspace/Shared/Research_Infra/Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure"
CAMINHO_MENSAGENS = f"{RAIZ_REPO}/MENSAGENS.md"

PASTA_SAIDA = "/Volumes/desafio_kinea/research/research_volume/relatorios"
os.makedirs(PASTA_SAIDA, exist_ok=True)

AGORA = datetime.now(timezone.utc).astimezone()
AGORA_ISO = AGORA.strftime("%Y-%m-%d")
AGORA_LEGIVEL = AGORA.strftime("%d/%m/%Y às %H:%M")

# COMMAND ----------

# =============================================================================
# Busca de dados
# =============================================================================

def buscar_fontes():
    return spark.sql(f"""
        SELECT source_id, nome_fonte, setor, status, tipo_categoria,
               importancia_original, granularidade_conteudo, confidence_tier,
               metodo_captura, notebooks_responsaveis, tasks_responsaveis,
               cadencia_real, notas, coberta_por, grupo_exibicao,
               ultima_execucao, docs_capturados, ultimo_erro
        FROM {CATALOGO}.{SCHEMA}.{PREFIXO}fontes
    """).toPandas()


def buscar_notebooks():
    return spark.sql(f"""
        SELECT notebook_nome, caminho_repo, padrao_arquitetura, parametrizado,
               fontes_cobertas, dependencias_externas, notas
        FROM {CATALOGO}.{SCHEMA}.{PREFIXO}notebooks
    """).toPandas()


def buscar_tasks():
    return spark.sql(f"""
        SELECT job, nome_task, notebook, parametro_fonte, modo_execucao,
               cluster, depends_on, horario_agendado
        FROM {CATALOGO}.{SCHEMA}.{PREFIXO}tasks
    """).toPandas()


def buscar_bloqueios():
    return spark.sql(f"""
        SELECT tipo, nome, fontes_afetadas, descricao, responsavel,
               status_ou_reversibilidade, notas
        FROM {CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios
    """).toPandas()


df_fontes = buscar_fontes()
df_notebooks = buscar_notebooks()
df_tasks = buscar_tasks()
df_bloqueios = buscar_bloqueios()

print(f"[dados] {len(df_fontes)} fontes, {len(df_notebooks)} notebooks, "
      f"{len(df_tasks)} tasks, {len(df_bloqueios)} bloqueios/descartes")

# COMMAND ----------

# =============================================================================
# Carrega MENSAGENS.md e separa por seção (título "## Nome da seção")
# =============================================================================

def normalizar(s: str) -> str:
    s = (s or "").lower().strip()
    for de, para in [("ã","a"),("á","a"),("â","a"),("ç","c"),("é","e"),
                      ("ê","e"),("í","i"),("ó","o"),("õ","o"),("ú","u")]:
        s = s.replace(de, para)
    return s


def carregar_mensagens(caminho: str) -> dict:
    if not os.path.exists(caminho):
        print(f"[aviso] {caminho} não encontrado — abas ficam sem anotação manual.")
        return {}

    secoes, atual, buffer = {}, None, []
    with open(caminho, encoding="utf-8") as f:
        for linha in f:
            linha = linha.rstrip("\n")
            if linha.startswith("## "):
                if atual is not None:
                    secoes[atual] = "\n".join(buffer).strip()
                atual = normalizar(linha[3:])
                buffer = []
            elif linha.startswith("# "):
                continue
            else:
                buffer.append(linha)
        if atual is not None:
            secoes[atual] = "\n".join(buffer).strip()

    # Descarta texto-placeholder do template, ex.: "(recado geral...)"
    for chave, texto in list(secoes.items()):
        if texto.startswith("(") and texto.endswith(")"):
            secoes[chave] = ""
    return secoes


MENSAGENS = carregar_mensagens(CAMINHO_MENSAGENS)

# COMMAND ----------

# =============================================================================
# Helpers
# =============================================================================

def escapar(texto) -> str:
    if texto is None:
        return ""
    texto = str(texto)
    if texto.strip().lower() in ("none", "nan", "nat", ""):
        return ""
    return texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def escapar_multilinha(texto) -> str:
    return escapar(texto).replace("\n", "<br>")


def status_para_classe(status: str) -> str:
    if not status:
        return "badge-medio"
    s = status.lower()
    if "validada em produção" in s or "coberta indiretamente" in s:
        return "badge-ok"
    if "erro" in s or "bloquead" in s:
        return "badge-erro"
    if "descartada" in s:
        return "badge-descartada"
    return "badge-medio"


def eh_coberta(status: str) -> bool:
    s = (status or "").lower()
    return "validada em produção" in s or "coberta indiretamente" in s


def formatar_data(valor) -> str:
    if valor is None or str(valor) in ("NaT", "None"):
        return "nunca rodou"
    try:
        return valor.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(valor)


def campo(label: str, valor) -> str:
    valor_escapado = escapar(valor)
    if not valor_escapado:
        return ""
    return f'<div class="ficha-campo"><span class="ficha-label">{escapar(label)}</span><span class="ficha-valor">{valor_escapado}</span></div>'


def caixa_anotacao(chave_mensagem: str) -> str:
    texto = MENSAGENS.get(chave_mensagem, "")
    if not texto:
        return ""
    return f"""
    <div class="anotacao-box">
      <div class="anotacao-tag">Anotação da equipe</div>
      <div class="anotacao-texto">{escapar_multilinha(texto)}</div>
    </div>"""

# COMMAND ----------

# =============================================================================
# Aba: Fontes — fichas compactas e expansíveis, agrupadas por importância
# =============================================================================

ORDEM_IMPORTANCIA = ["Alta", "Média", "Media", "Baixa"]


def ordenar_importancias(valores):
    presentes = list(valores)
    ordenados = [v for v in ORDEM_IMPORTANCIA if v in presentes]
    resto = sorted(v for v in presentes if v not in ORDEM_IMPORTANCIA and v)
    return ordenados + resto


def bloqueio_relacionado(nome_fonte: str, df_bloqueios) -> str:
    if df_bloqueios is None or len(df_bloqueios) == 0:
        return ""
    nome_lower = (nome_fonte or "").lower()
    achados = df_bloqueios[
        df_bloqueios["nome"].fillna("").str.lower().str.contains(nome_lower, na=False)
        | df_bloqueios["fontes_afetadas"].fillna("").str.lower().str.contains(nome_lower, na=False)
    ]
    if len(achados) == 0:
        return ""
    blocos = ""
    for _, b in achados.iterrows():
        rotulo = "Bloqueio" if b["tipo"] == "bloqueado" else "Descarte"
        blocos += f"""
        <div class="ficha-bloqueio">
          <strong>{rotulo}:</strong> {escapar(b['descricao'])}<br>
          <span class="texto-fraco">Responsável: {escapar(b['responsavel']) or '—'} · Status: {escapar(b['status_ou_reversibilidade'])}</span>
        </div>"""
    return blocos


def renderizar_ficha_normal(f, mapa_nomes_por_id, df_bloqueios) -> str:
    classe_status = status_para_classe(f["status"])
    resumo = escapar(f["setor"]) or "—"

    coberta_por_txt = ""
    coberta_por_id = escapar(f["coberta_por"])
    if coberta_por_id:
        nome_cobridora = mapa_nomes_por_id.get(coberta_por_id, coberta_por_id)
        coberta_por_txt = campo("Coberta via", nome_cobridora)

    campos = "".join([
        coberta_por_txt,
        campo("Setor", f["setor"]),
        campo("Tipo", f["tipo_categoria"]),
        campo("Confidence tier", f["confidence_tier"]),
        campo("Granularidade do conteúdo", f["granularidade_conteudo"]),
        campo("Método de captura", f["metodo_captura"]),
        campo("Cadência real observada", f["cadencia_real"]),
        campo("Notebook(s) responsável(is)", f["notebooks_responsaveis"]),
        campo("Task(s) responsável(is)", f["tasks_responsaveis"]),
        campo("Última execução", formatar_data(f["ultima_execucao"])),
        campo("Documentos na última execução", f["docs_capturados"]),
        campo("Último erro registrado", f["ultimo_erro"]),
    ])

    notas_html = ""
    notas = escapar(f["notas"])
    if notas:
        notas_html = f'<div class="ficha-notas"><strong>Notas:</strong> {notas}</div>'

    bloqueio_html = bloqueio_relacionado(f["nome_fonte"], df_bloqueios)

    return f"""
    <div class="ficha-fonte">
      <div class="ficha-header">
        <span class="ficha-nome">{escapar(f['nome_fonte'])}</span>
        <span class="badge {classe_status}">{escapar(f['status'])}</span>
      </div>
      <div class="ficha-resumo">{resumo}</div>
      <details class="ficha-detalhes">
        <summary>Ver detalhes</summary>
        <div class="ficha-grid">{campos}
        </div>{notas_html}{bloqueio_html}
      </details>
    </div>"""


def renderizar_ficha_agrupada(nome_grupo: str, linhas) -> str:
    primeira = linhas.iloc[0]
    classe_status = status_para_classe(primeira["status"])

    itens_grupo = "".join(
        f'<div class="grupo-item"><strong>{escapar(r["source_id"])}</strong> — {escapar(r["status"])}'
        f'{" — " + escapar(r["metodo_captura"]) if escapar(r["metodo_captura"]) else ""}</div>'
        for _, r in linhas.iterrows()
    )

    return f"""
    <div class="ficha-fonte">
      <div class="ficha-header">
        <span class="ficha-nome">{escapar(nome_grupo)}</span>
        <span class="badge {classe_status}">{len(linhas)} variações de captura</span>
      </div>
      <div class="ficha-resumo">Mesma fonte, capturada de {len(linhas)} formas diferentes — não são pendências separadas.</div>
      <details class="ficha-detalhes">
        <summary>Ver detalhes</summary>
        <div class="grupo-lista">{itens_grupo}
        </div>
      </details>
    </div>"""


def renderizar_aba_fontes(df, df_bloqueios) -> str:
    df = df.copy()
    mapa_nomes_por_id = dict(zip(df["source_id"], df["nome_fonte"]))

    # Descartadas saem da hierarquia de prioridade -- viram seção própria no final
    descartadas = df[df["status"].str.contains("descartada", case=False, na=False)]
    df = df[~df.index.isin(descartadas.index)]

    # Separa o que é exibido agrupado do restante
    agrupadas = df[df["grupo_exibicao"].notna() & (df["grupo_exibicao"].astype(str).str.strip() != "")]
    normais = df[~df.index.isin(agrupadas.index)].copy()

    # Segurança: qualquer fonte sem importância definida cai em "Baixa" por
    # padrão, em vez de virar uma seção "sem prioridade" própria -- o
    # esperado é que isso nunca aconteça (toda fonte deveria herdar uma
    # importância ao ser cadastrada), mas evita sumir silenciosamente caso
    # aconteça de novo no futuro.
    normais["importancia_original"] = normais["importancia_original"].apply(
        lambda v: "Baixa" if pd.isna(v) or not str(v).strip() else v
    )

    ordem = ordenar_importancias(normais["importancia_original"].unique())

    blocos = []
    for chave in ordem:
        subset = normais[normais["importancia_original"] == chave]
        titulo = f"Prioridade {chave}"

        if len(subset) == 0:
            continue

        # Cobertas primeiro
        subset = subset.assign(_coberta=subset["status"].apply(eh_coberta))
        subset = subset.sort_values("_coberta", ascending=False)

        fichas = "".join(renderizar_ficha_normal(f, mapa_nomes_por_id, df_bloqueios) for _, f in subset.iterrows())
        blocos.append(f"""
        <div class="section-label">{escapar(titulo)}</div>
        <div class="ficha-lista">{fichas}
        </div>""")

    # Grupos de captura (ex.: AGESAN) — seção própria, fora da hierarquia de importância
    if len(agrupadas) > 0:
        fichas_grupo = "".join(
            renderizar_ficha_agrupada(nome, linhas)
            for nome, linhas in agrupadas.groupby("grupo_exibicao")
        )
        blocos.append(f"""
        <div class="section-label">Fontes com múltiplas formas de captura</div>
        <div class="ficha-lista">{fichas_grupo}
        </div>""")

    # Descartadas — seção própria no final, separada das prioridades
    if len(descartadas) > 0:
        fichas_descartadas = "".join(
            renderizar_ficha_normal(f, mapa_nomes_por_id, df_bloqueios) for _, f in descartadas.iterrows()
        )
        blocos.append(f"""
        <div class="section-label">Fontes descartadas</div>
        <div class="ficha-lista">{fichas_descartadas}
        </div>""")

    blocos.append(caixa_anotacao("fontes"))
    return "\n".join(blocos)

# COMMAND ----------

# =============================================================================
# Aba: Notebooks & Tasks
# =============================================================================

def renderizar_aba_notebooks_tasks(df_notebooks, df_tasks) -> str:
    linhas_nb = "".join(f"""
        <tr>
          <td><strong>{escapar(n['notebook_nome'])}</strong></td>
          <td>{escapar(n['padrao_arquitetura'])}</td>
          <td>{escapar(n['fontes_cobertas'])}</td>
          <td>{escapar(n['dependencias_externas'])}</td>
          <td>{escapar(n['notas'])}</td>
        </tr>""" for _, n in df_notebooks.iterrows())

    linhas_tasks = "".join(f"""
        <tr>
          <td><strong>{escapar(t['job'])}</strong></td>
          <td>{escapar(t['nome_task'])}</td>
          <td>{escapar(t['notebook'])}</td>
          <td>{escapar(t['modo_execucao'])}</td>
          <td>{escapar(t['cluster'])}</td>
          <td>{escapar(t['horario_agendado'])}</td>
        </tr>""" for _, t in df_tasks.iterrows())

    return f"""
    <div class="section-label">Notebooks dispatchers</div>
    <table class="info-table">
      <thead><tr><th>Notebook</th><th>Padrão de arquitetura</th><th>Fontes cobertas</th><th>Dependências</th><th>Notas</th></tr></thead>
      <tbody>{linhas_nb}</tbody>
    </table>

    <div class="section-label">Tasks e agendamento</div>
    <table class="info-table">
      <thead><tr><th>Job</th><th>Task</th><th>Notebook</th><th>Modo de execução</th><th>Cluster</th><th>Horário</th></tr></thead>
      <tbody>{linhas_tasks}</tbody>
    </table>
    {caixa_anotacao("notebooks e tasks")}"""

# COMMAND ----------

# =============================================================================
# Aba: Bloqueios & Descartes
# =============================================================================

def renderizar_aba_bloqueios(df) -> str:
    if df is None or len(df) == 0:
        lista = '<div class="outro-item">Nenhum bloqueio ou descarte registrado.</div>'
    else:
        lista = ""
        for _, b in df.iterrows():
            rotulo = "Bloqueado" if b["tipo"] == "bloqueado" else "Descartado"
            classe = "badge-erro" if b["tipo"] == "bloqueado" else "badge-descartada"
            lista += f"""
            <div class="outro-item">
              <span class="badge {classe}">{rotulo}</span>
              <div>
                <strong>{escapar(b['nome'])}</strong> — {escapar(b['descricao'])}<br>
                <span class="texto-fraco">
                  Responsável: {escapar(b['responsavel']) or '—'} ·
                  Status/Reversibilidade: {escapar(b['status_ou_reversibilidade'])}
                  {(' · ' + escapar(b['notas'])) if escapar(b['notas']) else ''}
                </span>
              </div>
            </div>"""

    return f"""
    <div class="section-label">Bloqueios e fontes descartadas</div>
    <div class="outro-list">{lista}</div>
    {caixa_anotacao("bloqueios")}"""

# COMMAND ----------

# =============================================================================
# Aba: Fases & Entregáveis (rascunho superficial, baseado no briefing)
# =============================================================================

FASES_BRIEFING = [
    "Kickoff + arquitetura",
    "MVP das fontes P1 (sem NLP)",
    "Enriquecimento NLP + deduplicação",
    "Hardening + observabilidade",
    "Testes de aceitação + deploy",
]

ENTREGAVEIS_BRIEFING = [
    "Código-fonte do parser (fontes P1)",
    "Documentação técnica (README + arquitetura)",
    "Prompts LLM versionados",
    "Amostra de saída (mín. 100 notícias reais)",
    "Métricas de qualidade (precisão, dedup)",
    "Infra-as-code (opcional)",
    "Runbook operacional",
    "Estimativa de custo",
]


def renderizar_aba_fases() -> str:
    linhas_fases = "".join(f"""
        <tr><td>{i+1}</td><td>{escapar(fase)}</td><td><span class="badge badge-medio">A definir</span></td></tr>
    """ for i, fase in enumerate(FASES_BRIEFING))

    linhas_entregaveis = "".join(f"""
        <tr><td>{i+1}</td><td>{escapar(item)}</td><td><span class="badge badge-medio">A definir</span></td></tr>
    """ for i, item in enumerate(ENTREGAVEIS_BRIEFING))

    return f"""
    <div class="section-label">Fases do desafio (briefing, Seção 11)</div>
    <table class="info-table">
      <thead><tr><th>#</th><th>Fase</th><th>Status</th></tr></thead>
      <tbody>{linhas_fases}</tbody>
    </table>

    <div class="section-label">Entregáveis formais (briefing, Seção 9)</div>
    <table class="info-table">
      <thead><tr><th>#</th><th>Entregável</th><th>Status</th></tr></thead>
      <tbody>{linhas_entregaveis}</tbody>
    </table>
    <div class="standby-box">
      <div class="standby-tag">Rascunho</div>
      <div class="standby-texto">Status ainda não é rastreado individualmente — placeholder "A definir" até isso ser detalhado.</div>
    </div>
    {caixa_anotacao("fases")}"""

# COMMAND ----------

# =============================================================================
# Aba: Últimas atualizações
# =============================================================================

def renderizar_aba_atualizacoes(df, limite: int = 20) -> str:
    df = df.copy()
    df = df[df["ultima_execucao"].notna()].sort_values("ultima_execucao", ascending=False).head(limite)

    if len(df) == 0:
        itens = '<div class="outro-item">Nenhuma execução registrada ainda.</div>'
    else:
        itens = ""
        for _, f in df.iterrows():
            classe = status_para_classe(f["status"])
            quando = formatar_data(f["ultima_execucao"])
            erro_txt = f" — {escapar(f['ultimo_erro'])}" if escapar(f["ultimo_erro"]) else ""
            itens += f"""
            <div class="outro-item">
              <span class="badge {classe}">{escapar(f['status'])}</span>
              <div><span class="date-chip">{quando}</span> — <strong>{escapar(f['nome_fonte'])}</strong>{erro_txt}</div>
            </div>"""

    return f"""
    <div class="section-label">Últimas atualizações</div>
    <div class="outro-list">{itens}</div>
    {caixa_anotacao("atualizacoes")}"""

# COMMAND ----------

# =============================================================================
# Publicação no Azure Blob Storage + link temporário (SAS token)
#
# O link é gerado ANTES do upload de fato (a assinatura SAS não depende do
# blob já existir -- é só uma operação de assinatura com a account key), o
# que permite incluir o próprio link DENTRO do HTML que será publicado.
#
# As fichas de fonte usam <details>/<summary> nativo do HTML -- funciona
# bem em navegador/Gmail/Apple Mail, mas não em Outlook Desktop (motor do
# Word). Esse link dá a quem recebe o e-mail uma forma garantida de abrir
# a versão interativa de verdade, fora do cliente de e-mail.
#
# Se a conta de armazenamento não permitir gerar SAS token (ex.: acesso só
# por identidade gerenciada, sem account key exposta), isso falha de forma
# controlada -- o relatório continua sendo salvo e enviado normalmente,
# só sem o link interativo.
# =============================================================================

from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import timedelta

AZURE_CONTAINER_RELATORIOS = "relatorios-progresso"  # ajuste se o container real tiver outro nome
NOME_BLOB = f"relatorio_progresso_{AGORA_ISO}.html"
DIAS_VALIDADE_LINK = 30

LINK_INTERATIVO = None

try:
    _conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    _blob_service = BlobServiceClient.from_connection_string(_conn_str)
    _account_key = _blob_service.credential.account_key

    _sas_token = generate_blob_sas(
        account_name=_blob_service.account_name,
        container_name=AZURE_CONTAINER_RELATORIOS,
        blob_name=NOME_BLOB,
        account_key=_account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=DIAS_VALIDADE_LINK),
    )
    _blob_client = _blob_service.get_blob_client(container=AZURE_CONTAINER_RELATORIOS, blob=NOME_BLOB)
    LINK_INTERATIVO = f"{_blob_client.url}?{_sas_token}"
    print(f"[ok] Link interativo pronto (válido {DIAS_VALIDADE_LINK} dias): {LINK_INTERATIVO}")

except Exception as e:
    print(f"[aviso] Não foi possível preparar o link interativo do Blob Storage: {e}")
    print("[aviso] O relatório segue normalmente, só sem esse link.")

# COMMAND ----------

# =============================================================================
# Monta as seções
# =============================================================================
# Só "Fontes" fica -- "Bloqueios" e "Últimas atualizações" foram retiradas
# a pedido, para não sobrecarregar quando este relatório for combinado com
# o resumo semanal no mesmo documento.

ABAS = [
    ("fontes", "Fontes", renderizar_aba_fontes(df_fontes, df_bloqueios)),
]

# Seções empilhadas, uma após a outra -- sem clique, sem JS (mesmo padrão
# já adotado no briefing diário: nada de interatividade que dependa de
# JavaScript, já que quem abre isso num cliente de e-mail não teria acesso
# a ela mesmo assim).
secoes_html = "".join(
    f'<h2 class="report-section-heading">{escapar(titulo)}</h2>\n<div class="report-section">{conteudo}</div>'
    for _, titulo, conteudo in ABAS
)

# COMMAND ----------

# =============================================================================
# Template final
# =============================================================================

HTML_FINAL = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Relatório de Progresso — Kinea Research Infraestrutura</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    font-family: Georgia, 'Times New Roman', serif;
    font-size: 14px; line-height: 1.65; color: #2a2a2a; background: #eef1f7;
  }}

  .report-header {{
    background-color: #2d3b63;
    background: linear-gradient(135deg, #2d3b63 0%, #3d6b55 100%);
    color: #fff; padding: 24px 40px 22px 40px;
    border-bottom: 3px solid #c9a95c;
  }}
  .report-header .tag {{
    font-size: 11px; font-weight: 700; letter-spacing: 2px; text-transform: uppercase;
    color: #d8cba3 !important; margin-bottom: 6px;
  }}
  .report-header h1 {{ font-size: 24px; font-weight: 700; margin-bottom: 4px; color: #ffffff !important; }}
  .report-header .date {{ font-size: 14px; color: #e4ecff !important; font-weight: 600; }}

  .container {{ max-width: 980px; margin: 0 auto; padding: 28px 24px 60px 24px; }}

  .report-section-heading {{
    font-size: 20px; color: #2d3b63; margin: 32px 0 14px 0; padding-bottom: 8px;
    border-bottom: 2px solid #c9a95c;
  }}
  .report-section-heading:first-child {{ margin-top: 0; }}

  .section-label {{
    font-size: 10px; font-weight: 800; letter-spacing: 2.5px; text-transform: uppercase;
    color: #2d3b63; margin-bottom: 12px; margin-top: 30px; padding-bottom: 6px;
    border-bottom: 2px solid #e0dff5;
  }}
  .section-label:first-child {{ margin-top: 0; }}

  .texto-fraco {{ color: #8a87ad; font-size: 11px; }}

  /* Badges */
  .badge {{
    display: inline-block; font-size: 9.5px; font-weight: 800; letter-spacing: 0.3px;
    text-transform: uppercase; padding: 3px 8px; border-radius: 9px; white-space: nowrap;
  }}
  .badge-ok {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-erro {{ background: #fdecea; color: #c62828; }}
  .badge-medio {{ background: #fff8e1; color: #b28900; }}
  .badge-descartada {{ background: #f1f0f5; color: #6b6890; }}

  /* Fichas de fonte — compactas, em grade */
  .ficha-lista {{
    display: grid; grid-template-columns: repeat(auto-fill, minmax(230px, 1fr)); gap: 8px;
    align-items: start;
  }}
  .ficha-fonte {{
    background: #fff; border: 1px solid #e0ddf8; border-radius: 8px; padding: 10px 12px;
  }}
  .ficha-header {{ display: flex; align-items: center; justify-content: space-between; gap: 6px; }}
  .ficha-nome {{ font-size: 12px; font-weight: 800; color: #1a1855; }}
  .ficha-resumo {{ font-size: 10.5px; color: #8a87ad; margin-top: 2px; }}
  .ficha-detalhes {{ margin-top: 6px; }}
  .ficha-detalhes summary {{
    cursor: pointer; font-size: 10.5px; color: #5350a0; font-weight: 700; list-style: none;
  }}
  .ficha-detalhes summary::-webkit-details-marker {{ display: none; }}
  .ficha-detalhes summary::before {{ content: "▸ "; }}
  .ficha-detalhes[open] summary::before {{ content: "▾ "; }}
  .ficha-grid {{ margin-top: 8px; display: flex; flex-direction: column; gap: 3px; }}
  .ficha-campo {{ font-size: 11px; display: flex; gap: 5px; }}
  .ficha-label {{ color: #8a87ad; flex-shrink: 0; }}
  .ficha-label::after {{ content: ":"; }}
  .ficha-valor {{ color: #2c2a4a; }}
  .ficha-notas {{
    margin-top: 8px; padding-top: 8px; border-top: 1px dashed #ede9fb;
    font-size: 11px; color: #2c2a4a; line-height: 1.5;
  }}
  .ficha-bloqueio {{
    margin-top: 8px; background: #fffde7; border: 1px solid #ffe082; border-radius: 6px;
    padding: 6px 9px; font-size: 10.5px; line-height: 1.5;
  }}
  .grupo-lista {{ margin-top: 8px; display: flex; flex-direction: column; gap: 4px; }}
  .grupo-item {{ font-size: 11px; color: #2c2a4a; }}

  /* Tabelas */
  .info-table {{
    width: 100%; border-collapse: collapse; font-size: 12px;
    background: #fff; border-radius: 8px; overflow: hidden; border: 1px solid #e0ddf8;
  }}
  .info-table th {{
    background: #efecfb; color: #5350a0; font-size: 10px; font-weight: 800;
    letter-spacing: 0.6px; text-transform: uppercase; padding: 8px 10px; text-align: left;
    border-bottom: 2px solid #e0ddf8;
  }}
  .info-table td {{ padding: 8px 10px; border-bottom: 1px solid #f3f1fb; vertical-align: top; color: #2c2a4a; }}
  .info-table tr:last-child td {{ border-bottom: none; }}

  /* Listas simples */
  .outro-list {{ background: #fff; border: 1px solid #e0ddf8; border-radius: 8px; overflow: hidden; }}
  .outro-item {{
    display: flex; gap: 10px; padding: 10px 14px; border-bottom: 1px solid #f2f0fb;
    align-items: center; font-size: 12px; line-height: 1.5; color: #2c2a4a;
  }}
  .outro-item:last-child {{ border-bottom: none; }}
  .date-chip {{ font-weight: 700; color: #3d3a6e; font-variant-numeric: tabular-nums; }}

  /* Mensagem geral */
  .mensagem-geral {{
    background: #fff; border: 1px solid #e0ddf8; border-radius: 8px; padding: 16px 18px;
    font-size: 13px; line-height: 1.7; color: #2c2a4a;
  }}

  /* Caixa de anotação por seção */
  .anotacao-box {{
    margin-top: 24px; background: #fafafd; border: 1px dashed #d7d4ef; border-radius: 8px;
    padding: 12px 16px;
  }}
  .anotacao-tag {{
    font-size: 9.5px; font-weight: 800; letter-spacing: 0.5px; text-transform: uppercase;
    color: #5350a0; margin-bottom: 5px;
  }}
  .anotacao-texto {{ font-size: 12px; color: #4a4770; line-height: 1.6; }}

  .standby-box {{
    margin-top: 12px; background: #fafafd; border: 1px dashed #d7d4ef; border-radius: 8px;
    padding: 12px 16px; display: flex; align-items: center; gap: 12px;
  }}
  .standby-tag {{
    font-size: 9.5px; font-weight: 800; text-transform: uppercase; background: #ece9fb;
    color: #5350a0; padding: 3px 9px; border-radius: 9px; white-space: nowrap;
  }}
  .standby-texto {{ font-size: 11.5px; color: #6b6890; line-height: 1.5; }}

  .footer {{ text-align: center; font-size: 11px; color: #b0aecf; margin-top: 40px; padding-top: 16px; }}

  .link-interativo-banner {{
    background: #fff8e1; border: 1px solid #e0c68a; border-radius: 8px;
    padding: 12px 18px; font-size: 12.5px; color: #6b5a1e; margin-bottom: 20px;
  }}
  .link-interativo-banner a {{ color: #8a6d1a; font-weight: 700; text-decoration: underline; }}
</style>
</head>
<body>

<div class="report-header">
  <div class="tag">Kinea Investimentos</div>
  <h1>Relatório de Progresso do Projeto</h1>
  <div class="date">Gerado em {AGORA_LEGIVEL}</div>
</div>

<div class="container">
{f'<div class="link-interativo-banner">📄 Esta cópia estática não permite expandir os detalhes de cada fonte em todos os leitores de e-mail. <a href="{LINK_INTERATIVO}" target="_blank">Abra a versão interativa completa aqui</a> para ver tudo, com as fichas de fonte clicáveis.</div>' if LINK_INTERATIVO else ''}
{secoes_html}
  <div class="footer">Gerado automaticamente a partir das tabelas desafio_kinea.research.controle_* e de MENSAGENS.md</div>
</div>

</body>
</html>"""

# COMMAND ----------

# =============================================================================
# Salvar
# =============================================================================

nome_arquivo = f"relatorio_progresso_{AGORA_ISO}.html"
caminho_completo = os.path.join(PASTA_SAIDA, nome_arquivo)

with open(caminho_completo, "w", encoding="utf-8") as f:
    f.write(HTML_FINAL)

print(f"[ok] Relatório salvo em: {caminho_completo}")

# COMMAND ----------

# =============================================================================
# Upload de verdade para o Blob Storage -- feito DEPOIS do HTML_FINAL já
# ter o link embutido nele mesmo (o link foi gerado antes, sem depender do
# blob existir). Só roda se a preparação do link acima deu certo.
# =============================================================================

if LINK_INTERATIVO:
    try:
        from azure.storage.blob import ContentSettings

        _blob_client.upload_blob(
            HTML_FINAL.encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="text/html; charset=utf-8"),
        )
        print(f"[ok] Publicado no Blob Storage: {NOME_BLOB}")

        # Ponteiro pro link mais recente -- lido pelo
        # montar_resumo_semanal_completo.py, que não sabe (nem precisa
        # saber) como o link foi construído, só onde encontrar o mais
        # atual.
        _caminho_ponteiro = os.path.join(PASTA_SAIDA, "ultimo_link_relatorio.json")
        with open(_caminho_ponteiro, "w", encoding="utf-8") as f:
            json.dump({"link": LINK_INTERATIVO, "gerado_em": AGORA_LEGIVEL}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[aviso] Falha ao publicar no Blob Storage: {e}")
        LINK_INTERATIVO = None

# COMMAND ----------

# =============================================================================
# Envio por e-mail — reaproveita o mesmo mecanismo do briefing diário
# (Processa_Daily_Infra.ipynb): POST para o endpoint em URL_EMAIL, com o
# HTML como corpo e também como anexo.
#
# Ajuste ENVIAR_EMAIL / EMAILS_DESTINATARIOS conforme necessário. Isso não
# depende de nenhum comportamento do serviço externo de LLM (ui-agents) —
# é o mesmo endpoint interno de disparo de e-mail já usado em produção.
# =============================================================================

import json as _json
import requests

ENVIAR_EMAIL = True

EMAILS_DESTINATARIOS = [
    "marcos.markevich@kinea.com.br",
    "chrisaraujofsz@gmail.com",
    # adicione/remova destinatários aqui
]

if ENVIAR_EMAIL:
    try:
        _url_email = os.environ["URL_EMAIL"]
        for _email in EMAILS_DESTINATARIOS:
            _payload = _json.dumps({
                "body": f"<p>Relatório de progresso semanal do projeto de Infraestrutura, gerado em {AGORA_LEGIVEL}.</p>"
                        f"<p>O relatório completo está em anexo.</p>",
                "to": _email,
                "subject": f"Relatório de Progresso - Infraestrutura - {AGORA_ISO}",
                "attachmentPath": [caminho_completo],
                "attachment_sta": "staceokna",
            })
            _headers = {"Content-Type": "application/json"}
            _resposta = requests.request("POST", _url_email, headers=_headers, data=_payload, allow_redirects=False)
            print(f"[email] enviado para {_email}: status {_resposta.status_code}")
    except Exception as e:
        print(f"[aviso] Falha ao enviar e-mail (URL_EMAIL não configurada ou outro erro): {e}")
        print("[aviso] O relatório foi gerado e salvo normalmente -- só o envio falhou.")
else:
    print("[email] ENVIAR_EMAIL=False -- pulado")
