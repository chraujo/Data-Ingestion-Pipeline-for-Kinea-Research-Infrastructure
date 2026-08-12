# Databricks notebook source
# =============================================================================
# gerar_relatorio_progresso.py
#
# Relatório de progresso do projeto, em formato de abas dentro de um único
# arquivo HTML. Lê das tabelas de controle + do arquivo MENSAGENS.md (no
# repo) para anotações manuais por seção.
#
# IMPORTANTE: rode scripts/corrigir_dados_relatorio.py pelo menos uma vez
# antes deste notebook — ele cria as colunas `coberta_por` e
# `grupo_exibicao` que este relatório espera encontrar.
#
# Abas do relatório:
#   1. Mensagens        — recado geral (de MENSAGENS.md)
#   2. Fontes            — fichas compactas expansíveis, por importância
#                           (Alta/Média/Baixa), cobertas primeiro
#   3. Notebooks & Tasks  — tabelas de dispatchers e agendamento
#   4. Bloqueios          — bloqueios e fontes descartadas
#   5. Fases & Entregáveis— rascunho superficial baseado no briefing
#   6. Últimas atualizações
#
# Cada aba tem, no rodapé, um espaço de anotação livre (texto de
# MENSAGENS.md daquela seção específica).
# =============================================================================

# COMMAND ----------

import os
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
# Monta as abas
# =============================================================================
# Abas "Mensagens", "Notebooks & Tasks" e "Fases & Entregáveis" foram
# removidas a pedido -- notebook/método de captura de cada fonte já
# aparecem dentro da própria ficha (campo() já inclui isso desde a
# reformulação anterior), então a aba separada de Notebooks & Tasks ficou
# redundante.

ABAS = [
    ("fontes", "Fontes", renderizar_aba_fontes(df_fontes, df_bloqueios)),
    ("bloqueios", "Bloqueios", renderizar_aba_bloqueios(df_bloqueios)),
    ("atualizacoes", "Últimas atualizações", renderizar_aba_atualizacoes(df_fontes)),
]

nav_html = "".join(
    f'<button class="tab-btn{" active" if i == 0 else ""}" onclick="mostrarAba(\'{chave}\')" id="btn-{chave}">{escapar(titulo)}</button>'
    for i, (chave, titulo, _) in enumerate(ABAS)
)

paineis_html = "".join(
    f'<div class="tab-panel{" active" if i == 0 else ""}" id="painel-{chave}">{conteudo}</div>'
    for i, (chave, _, conteudo) in enumerate(ABAS)
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
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 14px; line-height: 1.6; color: #1a1a2e; background: #f4f3fb;
  }}

  .report-header {{
    background: linear-gradient(135deg, #0e1729 0%, #1a1855 70%, #25226e 100%);
    color: #fff; padding: 30px 40px 0 40px;
  }}
  .report-header .tag {{
    font-size: 10px; font-weight: 700; letter-spacing: 2.5px; text-transform: uppercase;
    color: #a89fd8; margin-bottom: 6px;
  }}
  .report-header h1 {{ font-size: 22px; font-weight: 700; letter-spacing: -0.3px; margin-bottom: 3px; }}
  .report-header .date {{ font-size: 12px; color: #b8b0e0; margin-bottom: 18px; }}

  .tab-nav {{ display: flex; gap: 4px; overflow-x: auto; padding-top: 6px; }}
  .tab-btn {{
    background: transparent; border: none; color: #b8b0e0; font-size: 12px; font-weight: 700;
    padding: 10px 16px; cursor: pointer; white-space: nowrap; border-bottom: 3px solid transparent;
    transition: color .15s, border-color .15s;
  }}
  .tab-btn:hover {{ color: #fff; }}
  .tab-btn.active {{ color: #fff; border-bottom-color: #4ba3c7; }}

  .container {{ max-width: 980px; margin: 0 auto; padding: 28px 24px 60px 24px; }}

  .tab-panel {{ display: none; }}
  .tab-panel.active {{ display: block; }}

  .section-label {{
    font-size: 10px; font-weight: 800; letter-spacing: 2.5px; text-transform: uppercase;
    color: #5350a0; margin-bottom: 12px; margin-top: 30px; padding-bottom: 6px;
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
</style>
</head>
<body>

<div class="report-header">
  <div class="tag">Kinea Research · Infraestrutura</div>
  <h1>Relatório de Progresso do Projeto</h1>
  <div class="date">Gerado em {AGORA_LEGIVEL}</div>
  <div class="tab-nav">{nav_html}</div>
</div>

<div class="container">
{paineis_html}
  <div class="footer">Gerado automaticamente a partir das tabelas desafio_kinea.research.controle_* e de MENSAGENS.md</div>
</div>

<script>
function mostrarAba(chave) {{
  document.querySelectorAll('.tab-panel').forEach(function(p) {{ p.classList.remove('active'); }});
  document.querySelectorAll('.tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
  document.getElementById('painel-' + chave).classList.add('active');
  document.getElementById('btn-' + chave).classList.add('active');
}}
</script>

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
        _url_email = os.environ["URL_EMAIL"]
        _resposta = requests.request("POST", _url_email, headers=_headers, data=_payload, allow_redirects=False)
        print(f"[email] enviado para {_email}: status {_resposta.status_code}")
else:
    print("[email] ENVIAR_EMAIL=False -- pulado")
