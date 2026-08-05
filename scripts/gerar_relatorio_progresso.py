# Databricks notebook source
# =============================================================================
# gerar_relatorio_progresso.py
#
# Etapa 5: gera um relatório HTML de progresso do projeto, lendo direto das
# tabelas de controle — sem edição manual.
#
# Reaproveita o mesmo sistema visual do exemplo_resultado_infra.html (o
# briefing diário que o pipeline já produz), pra manter consistência de marca
# entre os dois documentos que o time recebe.
#
# Estrutura do relatório:
#   - Placar geral
#   - Fontes por nível de importância (Alta / Média / Baixa), com ficha
#     completa de cada uma (status, método de captura, confidence tier,
#     cadência, notebooks/tasks responsáveis, notas, bloqueio associado
#     se houver, e o resultado da última execução real)
#   - Notebooks dispatchers (o que cada um cobre, arquitetura, dependências)
#   - Tasks / agendamento (jobs, cluster, horário)
#   - Bloqueios e fontes descartadas
#   - Últimas atualizações
#   - Seções em standby (entregáveis, critérios, timeline, custo — chegam
#     quando o projeto tiver essa informação madura)
#
# Rode este notebook sempre que quiser um retrato atualizado — sob demanda,
# ou agendado como um Job.
# =============================================================================

# COMMAND ----------

import os
from datetime import datetime, timezone

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

PASTA_SAIDA = "/Volumes/desafio_kinea/research/research_volume/relatorios"
os.makedirs(PASTA_SAIDA, exist_ok=True)

AGORA = datetime.now(timezone.utc).astimezone()
AGORA_ISO = AGORA.strftime("%Y-%m-%d")
AGORA_LEGIVEL = AGORA.strftime("%d/%m/%Y às %H:%M")

# COMMAND ----------

# =============================================================================
# Flags de seção — troque para True quando a seção estiver pronta pra ativar.
# =============================================================================

SECOES = {
    "placar_geral":          True,
    "fontes_por_importancia":True,
    "notebooks_dispatchers": True,
    "tasks_agendamento":     True,
    "bloqueios_descartes":   True,
    "ultimas_atualizacoes":  True,
    "entregaveis":           False,
    "criterios_aceitacao":   False,
    "timeline_fases":        False,
    "estimativa_custo":      False,
}

# COMMAND ----------

# =============================================================================
# Busca de dados
# =============================================================================

def buscar_fontes():
    return spark.sql(f"""
        SELECT source_id, nome_fonte, setor, status, tipo_categoria,
               importancia_original, granularidade_conteudo, confidence_tier,
               metodo_captura, notebooks_responsaveis, tasks_responsaveis,
               cadencia_real, notas,
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


df_fontes = buscar_fontes() if (SECOES["placar_geral"] or SECOES["fontes_por_importancia"] or SECOES["ultimas_atualizacoes"]) else None
df_notebooks = buscar_notebooks() if SECOES["notebooks_dispatchers"] else None
df_tasks = buscar_tasks() if SECOES["tasks_agendamento"] else None
df_bloqueios = buscar_bloqueios() if (SECOES["bloqueios_descartes"] or SECOES["fontes_por_importancia"]) else None

print(f"[dados] {len(df_fontes) if df_fontes is not None else 0} fontes, "
      f"{len(df_notebooks) if df_notebooks is not None else 0} notebooks, "
      f"{len(df_tasks) if df_tasks is not None else 0} tasks, "
      f"{len(df_bloqueios) if df_bloqueios is not None else 0} bloqueios/descartes")

# COMMAND ----------

# =============================================================================
# Helpers
# =============================================================================

def escapar(texto) -> str:
    if texto is None:
        return ""
    texto = str(texto)
    if texto.strip().lower() in ("none", "nan", "nat"):
        return ""
    return texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def status_para_classe(status: str) -> str:
    if not status:
        return "badge-medio"
    s = status.lower()
    if "validada em produção" in s:
        return "badge-ok"
    if "erro" in s or "bloquead" in s:
        return "badge-erro"
    if "descartada" in s:
        return "badge-descartada"
    return "badge-medio"


def formatar_data(valor) -> str:
    if valor is None or str(valor) in ("NaT", "None"):
        return "nunca rodou"
    try:
        return valor.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(valor)


def campo(label: str, valor) -> str:
    """Um par label/valor dentro da ficha de uma fonte. Omite se vazio."""
    valor_escapado = escapar(valor)
    if not valor_escapado:
        return ""
    return f'<div class="ficha-campo"><span class="ficha-label">{escapar(label)}</span><span class="ficha-valor">{valor_escapado}</span></div>'

# COMMAND ----------

# =============================================================================
# Renderização — Placar geral
# =============================================================================

def renderizar_placar_geral(df) -> str:
    total = len(df)
    validadas = (df["status"].str.contains("validada em produção", case=False, na=False)).sum()
    com_erro = (df["status"].str.contains("erro|bloquead", case=False, na=False, regex=True)).sum()
    pct_cobertura = round(100 * validadas / total) if total else 0

    cards = [
        ("Fontes catalogadas", total, "#5350a0"),
        ("Cobertas e validadas", validadas, "#66bb6a"),
        ("Com erro ou bloqueio ativo", com_erro, "#ef5350"),
        ("Cobertura geral", f"{pct_cobertura}%", "#1a1855"),
    ]

    html_cards = "".join(f"""
        <div class="placar-card">
          <div class="placar-valor" style="color:{cor}">{valor}</div>
          <div class="placar-label">{escapar(label)}</div>
        </div>""" for label, valor, cor in cards)

    return f"""
    <div class="section-label">Placar geral</div>
    <div class="placar-grid">{html_cards}
    </div>"""

# COMMAND ----------

# =============================================================================
# Renderização — Fontes por nível de importância, com ficha completa
# =============================================================================

ORDEM_IMPORTANCIA = ["Alta", "Média", "Media", "Baixa"]

def ordenar_importancias(valores):
    presentes = list(valores)
    ordenados = [v for v in ORDEM_IMPORTANCIA if v in presentes]
    resto = sorted(v for v in presentes if v not in ORDEM_IMPORTANCIA and v)
    sem_definicao = [v for v in presentes if not v or str(v).strip() == ""]
    return ordenados + resto + (["__sem_definicao__"] if sem_definicao else [])


def bloqueio_relacionado(nome_fonte: str, df_bloqueios) -> str:
    """Procura, na tabela de bloqueios/descartes, alguma entrada que cite essa fonte."""
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
          <span style="color:#8a87ad;font-size:11px;">
            Responsável: {escapar(b['responsavel']) or '—'} · Status: {escapar(b['status_ou_reversibilidade'])}
          </span>
        </div>"""
    return blocos


def renderizar_ficha_fonte(f, df_bloqueios) -> str:
    classe_status = status_para_classe(f["status"])
    campos = "".join([
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
        notas_html = f'<div class="ficha-notas"><strong>Notas / dificuldades:</strong> {notas}</div>'

    bloqueio_html = bloqueio_relacionado(f["nome_fonte"], df_bloqueios)

    return f"""
    <div class="ficha-fonte">
      <div class="ficha-header">
        <div class="ficha-nome">{escapar(f['nome_fonte'])}</div>
        <div class="badge {classe_status}">{escapar(f['status'])}</div>
      </div>
      <div class="ficha-grid">{campos}
      </div>{notas_html}{bloqueio_html}
    </div>"""


def renderizar_fontes_por_importancia(df, df_bloqueios) -> str:
    df = df.copy()
    grupos = df.groupby("importancia_original", dropna=False)
    ordem = ordenar_importancias(df["importancia_original"].fillna("").unique())

    blocos = []
    for chave in ordem:
        if chave == "__sem_definicao__":
            subset = df[df["importancia_original"].isna() | (df["importancia_original"].str.strip() == "")]
            titulo = "Sem prioridade definida"
        else:
            subset = df[df["importancia_original"] == chave]
            titulo = f"Prioridade {chave}"

        if len(subset) == 0:
            continue

        fichas = "".join(renderizar_ficha_fonte(f, df_bloqueios) for _, f in subset.iterrows())
        blocos.append(f"""
        <div class="section-label">{escapar(titulo)} ({len(subset)} fontes)</div>
        <div class="ficha-lista">{fichas}
        </div>""")

    return "\n".join(blocos)

# COMMAND ----------

# =============================================================================
# Renderização — Notebooks dispatchers
# =============================================================================

def renderizar_notebooks(df) -> str:
    linhas = ""
    for _, n in df.iterrows():
        linhas += f"""
        <tr>
          <td><strong>{escapar(n['notebook_nome'])}</strong></td>
          <td>{escapar(n['padrao_arquitetura'])}</td>
          <td>{escapar(n['fontes_cobertas'])}</td>
          <td>{escapar(n['dependencias_externas'])}</td>
          <td>{escapar(n['notas'])}</td>
        </tr>"""

    return f"""
    <div class="section-label">Notebooks dispatchers</div>
    <table class="info-table">
      <thead>
        <tr><th>Notebook</th><th>Padrão de arquitetura</th><th>Fontes cobertas</th><th>Dependências</th><th>Notas</th></tr>
      </thead>
      <tbody>{linhas}
      </tbody>
    </table>"""

# COMMAND ----------

# =============================================================================
# Renderização — Tasks / agendamento
# =============================================================================

def renderizar_tasks(df) -> str:
    linhas = ""
    for _, t in df.iterrows():
        linhas += f"""
        <tr>
          <td><strong>{escapar(t['job'])}</strong></td>
          <td>{escapar(t['nome_task'])}</td>
          <td>{escapar(t['notebook'])}</td>
          <td>{escapar(t['modo_execucao'])}</td>
          <td>{escapar(t['cluster'])}</td>
          <td>{escapar(t['horario_agendado'])}</td>
        </tr>"""

    return f"""
    <div class="section-label">Tasks e agendamento</div>
    <table class="info-table">
      <thead>
        <tr><th>Job</th><th>Task</th><th>Notebook</th><th>Modo de execução</th><th>Cluster</th><th>Horário</th></tr>
      </thead>
      <tbody>{linhas}
      </tbody>
    </table>"""

# COMMAND ----------

# =============================================================================
# Renderização — Bloqueios e fontes descartadas
# =============================================================================

def renderizar_bloqueios_descartes(df) -> str:
    if df is None or len(df) == 0:
        return """
    <div class="section-label">Bloqueios e fontes descartadas</div>
    <div class="outro-list"><div class="outro-item">Nenhum bloqueio ou descarte registrado.</div></div>"""

    itens = ""
    for _, b in df.iterrows():
        rotulo = "Bloqueado" if b["tipo"] == "bloqueado" else "Descartado"
        classe = "badge-erro" if b["tipo"] == "bloqueado" else "badge-descartada"
        itens += f"""
        <div class="outro-item">
          <div class="badge {classe}" style="margin-top:1px;">{rotulo}</div>
          <div>
            <strong>{escapar(b['nome'])}</strong> — {escapar(b['descricao'])}<br>
            <span style="color:#8a87ad;font-size:11px;">
              Responsável: {escapar(b['responsavel']) or '—'} ·
              Status/Reversibilidade: {escapar(b['status_ou_reversibilidade'])}
              {(' · Notas: ' + escapar(b['notas'])) if escapar(b['notas']) else ''}
            </span>
          </div>
        </div>"""

    return f"""
    <div class="section-label">Bloqueios e fontes descartadas</div>
    <div class="outro-list">{itens}
    </div>"""

# COMMAND ----------

# =============================================================================
# Renderização — Últimas atualizações
# =============================================================================

def renderizar_ultimas_atualizacoes(df, limite: int = 15) -> str:
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
              <div class="badge {classe}" style="margin-top:1px;">{escapar(f['status'])}</div>
              <div>
                <span class="date-chip">{quando}</span> —
                <strong>{escapar(f['nome_fonte'])}</strong>{erro_txt}
              </div>
            </div>"""

    return f"""
    <div class="section-label">Últimas atualizações</div>
    <div class="outro-list">{itens}
    </div>"""

# COMMAND ----------

# =============================================================================
# Renderização — placeholder para seções em standby
# =============================================================================

def renderizar_standby(titulo: str, descricao: str) -> str:
    return f"""
    <div class="section-label">{escapar(titulo)}</div>
    <div class="standby-box">
      <div class="standby-tag">Em preparação</div>
      <div class="standby-texto">{escapar(descricao)}</div>
    </div>"""

# COMMAND ----------

# =============================================================================
# Monta o corpo do relatório, seção por seção
# =============================================================================

blocos = []

if SECOES["placar_geral"]:
    blocos.append(renderizar_placar_geral(df_fontes))

if SECOES["fontes_por_importancia"]:
    blocos.append(renderizar_fontes_por_importancia(df_fontes, df_bloqueios))

if SECOES["notebooks_dispatchers"]:
    blocos.append(renderizar_notebooks(df_notebooks))

if SECOES["tasks_agendamento"]:
    blocos.append(renderizar_tasks(df_tasks))

if SECOES["bloqueios_descartes"]:
    blocos.append(renderizar_bloqueios_descartes(df_bloqueios))

if SECOES["ultimas_atualizacoes"]:
    blocos.append(renderizar_ultimas_atualizacoes(df_fontes))

if not SECOES["entregaveis"]:
    blocos.append(renderizar_standby(
        "Entregáveis do desafio",
        "Chega quando os 8 itens da Seção 9 do briefing (código, docs, prompts, "
        "amostra JSON, métricas, IaC, runbook, custo) tiverem status individual "
        "rastreado."
    ))

if not SECOES["criterios_aceitacao"]:
    blocos.append(renderizar_standby(
        "Critérios de aceitação",
        "Chega com a autoavaliação de cada critério da Seção 10 do briefing "
        "(conformidade de schema, qualidade NLP, dedup, cobertura, resiliência, "
        "documentação) e seus pesos oficiais."
    ))

if not SECOES["timeline_fases"]:
    blocos.append(renderizar_standby(
        "Timeline do projeto",
        "Chega com as 5 fases da Seção 11 do briefing (Kickoff, MVP P1, "
        "NLP+Dedup, Hardening, Testes de aceitação), data alvo vs. realizada."
    ))

if not SECOES["estimativa_custo"]:
    blocos.append(renderizar_standby(
        "Estimativa de custo",
        "Chega com o resumo da planilha de custo (LLM + storage + compute) "
        "exigida no item 8 dos entregáveis formais."
    ))

corpo_html = "\n".join(blocos)

# COMMAND ----------

# =============================================================================
# Template final — reaproveita o sistema visual do briefing diário
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
    font-size: 14px;
    line-height: 1.65;
    color: #1a1a2e;
    background: #f4f3fb;
  }}

  .report-header {{
    background: linear-gradient(135deg, #0e1729 0%, #1a1855 70%, #25226e 100%);
    color: #fff;
    padding: 38px 52px 30px 52px;
  }}
  .report-header .tag {{
    font-size: 10px; font-weight: 700; letter-spacing: 2.5px;
    text-transform: uppercase; color: #a89fd8; margin-bottom: 6px;
  }}
  .report-header h1 {{ font-size: 24px; font-weight: 700; letter-spacing: -0.3px; margin-bottom: 3px; }}
  .report-header .date {{ font-size: 13px; color: #b8b0e0; }}
  .accent-rule {{
    height: 2px;
    background: linear-gradient(90deg, #4ba3c7, #5350a0, transparent);
    margin: 16px 0 0 0;
  }}

  .container {{ max-width: 980px; margin: 0 auto; padding: 32px 24px 60px 24px; }}

  .section-label {{
    font-size: 10px; font-weight: 800; letter-spacing: 2.5px; text-transform: uppercase;
    color: #5350a0; margin-bottom: 14px; margin-top: 36px; padding-bottom: 7px;
    border-bottom: 2px solid #e0dff5;
  }}
  .section-label:first-child {{ margin-top: 0; }}

  /* Placar */
  .placar-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }}
  .placar-card {{
    background: #fff; border: 1px solid #e0ddf8; border-radius: 12px;
    padding: 18px 14px; text-align: center;
  }}
  .placar-valor {{ font-size: 26px; font-weight: 800; }}
  .placar-label {{ font-size: 11px; color: #6b6890; margin-top: 4px; line-height: 1.4; }}

  /* Badges de status */
  .badge {{
    display: inline-block; font-size: 10px; font-weight: 800; letter-spacing: 0.4px;
    text-transform: uppercase; padding: 3px 9px; border-radius: 10px; white-space: nowrap;
  }}
  .badge-ok {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-erro {{ background: #fdecea; color: #c62828; }}
  .badge-medio {{ background: #fff8e1; color: #b28900; }}
  .badge-descartada {{ background: #f1f0f5; color: #6b6890; }}

  /* Ficha de fonte */
  .ficha-lista {{ display: flex; flex-direction: column; gap: 12px; }}
  .ficha-fonte {{
    background: #fff; border: 1px solid #e0ddf8; border-radius: 12px; padding: 16px 20px;
  }}
  .ficha-header {{
    display: flex; align-items: center; justify-content: space-between;
    gap: 10px; margin-bottom: 10px;
  }}
  .ficha-nome {{ font-size: 14px; font-weight: 800; color: #1a1855; }}
  .ficha-grid {{
    display: grid; grid-template-columns: 1fr 1fr; gap: 6px 24px;
  }}
  .ficha-campo {{ font-size: 12px; display: flex; gap: 6px; padding: 3px 0; }}
  .ficha-label {{ color: #8a87ad; flex-shrink: 0; }}
  .ficha-label::after {{ content: ":"; }}
  .ficha-valor {{ color: #2c2a4a; }}
  .ficha-notas {{
    margin-top: 10px; padding-top: 10px; border-top: 1px dashed #ede9fb;
    font-size: 12.5px; color: #2c2a4a; line-height: 1.6;
  }}
  .ficha-bloqueio {{
    margin-top: 10px; background: #fffde7; border: 1px solid #ffe082; border-radius: 7px;
    padding: 8px 12px; font-size: 12px; line-height: 1.5;
  }}

  /* Tabelas (notebooks, tasks) */
  .info-table {{
    width: 100%; border-collapse: collapse; font-size: 12px;
    background: #fff; border-radius: 10px; overflow: hidden; border: 1px solid #e0ddf8;
  }}
  .info-table th {{
    background: #efecfb; color: #5350a0; font-size: 10px; font-weight: 800;
    letter-spacing: 0.8px; text-transform: uppercase; padding: 9px 12px; text-align: left;
    border-bottom: 2px solid #e0ddf8;
  }}
  .info-table td {{
    padding: 9px 12px; border-bottom: 1px solid #f3f1fb; vertical-align: top; color: #2c2a4a;
  }}
  .info-table tr:last-child td {{ border-bottom: none; }}
  .info-table tr:hover td {{ background: #faf9ff; }}

  /* Listas (bloqueios, últimas atualizações) */
  .outro-list {{ background: #fff; border: 1px solid #e0ddf8; border-radius: 10px; overflow: hidden; }}
  .outro-item {{
    display: flex; gap: 12px; padding: 12px 18px; border-bottom: 1px solid #f2f0fb;
    align-items: flex-start; font-size: 12.5px; line-height: 1.6; color: #2c2a4a;
  }}
  .outro-item:last-child {{ border-bottom: none; }}
  .date-chip {{ font-weight: 700; color: #3d3a6e; font-variant-numeric: tabular-nums; }}

  /* Standby */
  .standby-box {{
    background: #fafafd; border: 1px dashed #d7d4ef; border-radius: 10px;
    padding: 16px 18px; display: flex; align-items: center; gap: 14px;
  }}
  .standby-tag {{
    font-size: 10px; font-weight: 800; letter-spacing: 0.6px; text-transform: uppercase;
    background: #ece9fb; color: #5350a0; padding: 4px 10px; border-radius: 10px;
    white-space: nowrap; flex-shrink: 0;
  }}
  .standby-texto {{ font-size: 12.5px; color: #6b6890; line-height: 1.6; }}

  .footer {{ text-align: center; font-size: 11px; color: #b0aecf; margin-top: 48px; padding-top: 18px; }}

  @media (max-width: 640px) {{
    .placar-grid {{ grid-template-columns: 1fr 1fr; }}
    .ficha-grid {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<div class="report-header">
  <div class="tag">Kinea Research · Infraestrutura</div>
  <h1>Relatório de Progresso do Projeto</h1>
  <div class="date">Gerado em {AGORA_LEGIVEL}</div>
  <div class="accent-rule"></div>
</div>

<div class="container">
{corpo_html}

  <div class="footer">Gerado automaticamente a partir das tabelas desafio_kinea.research.controle_*</div>
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
