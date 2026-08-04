# Databricks notebook source
# =============================================================================
# gerar_relatorio_progresso.py
#
# Etapa 5: gera um relatório HTML de progresso do projeto, lendo direto da
# tabela controle_fontes (e controle_bloqueios) — sem edição manual.
#
# Reaproveita o mesmo sistema visual do exemplo_resultado_infra.html (o
# briefing diário que o pipeline já produz), pra manter consistência de marca
# entre os dois documentos que o time recebe.
#
# Seções ATIVAS (dados 100% automáticos, vindos das tabelas):
#   - Placar geral
#   - Cobertura por fonte/setor
#   - Pendências e bloqueios conhecidos
#   - Últimas atualizações
#
# Seções em STANDBY (aguardando informação que só existe mais adiante no
# projeto — entregáveis formais, critérios de aceitação, timeline, custo):
#   renderizam um card "Em preparação", não desaparecem, só ainda não têm dado.
#
# Rode este notebook sempre que quiser um retrato atualizado — sob demanda,
# ou agendado como um Job (ex.: toda segunda de manhã, antes do standup).
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
# Não precisa mexer em mais nada do script pra isso: só escrever a função
# `buscar_*` correspondente (hoje retornando None) e virar o flag.
# =============================================================================

SECOES = {
    "placar_geral":        True,
    "cobertura_fontes":    True,
    "pendencias":          True,
    "ultimas_atualizacoes":True,
    "entregaveis":         False,
    "criterios_aceitacao": False,
    "timeline_fases":      False,
    "estimativa_custo":    False,
}

# COMMAND ----------

# =============================================================================
# Busca de dados (só roda para as seções ativas)
# =============================================================================

def buscar_fontes():
    return spark.sql(f"""
        SELECT source_id, nome_fonte, setor, status, metodo_captura,
               ultima_execucao, docs_capturados, ultimo_erro
        FROM {CATALOGO}.{SCHEMA}.{PREFIXO}fontes
    """).toPandas()


def buscar_bloqueios():
    return spark.sql(f"""
        SELECT tipo, nome, descricao, responsavel, status_ou_reversibilidade
        FROM {CATALOGO}.{SCHEMA}.{PREFIXO}bloqueios
        WHERE tipo = 'bloqueado'
    """).toPandas()


df_fontes = buscar_fontes() if any([SECOES["placar_geral"], SECOES["cobertura_fontes"], SECOES["ultimas_atualizacoes"]]) else None
df_bloqueios = buscar_bloqueios() if SECOES["pendencias"] else None

print(f"[dados] {len(df_fontes) if df_fontes is not None else 0} fontes, "
      f"{len(df_bloqueios) if df_bloqueios is not None else 0} bloqueios ativos")

# COMMAND ----------

# =============================================================================
# Helpers de classificação visual
# =============================================================================

def status_para_sinal(status: str) -> str:
    """Mapeia o status textual para a cor do 'sinal' (bolinha) do card."""
    if not status:
        return "signal-medium"
    s = status.lower()
    if "validada em produção" in s:
        return "signal-low"       # verde = ok
    if "parcial" in s or "não testada" in s:
        return "signal-medium"    # amarelo = atenção
    if "erro" in s or "bloquead" in s:
        return "signal-high"      # vermelho = problema
    return "signal-medium"


def escapar(texto) -> str:
    if texto is None:
        return ""
    return (
        str(texto)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

# COMMAND ----------

# =============================================================================
# Renderização — Placar geral
# =============================================================================

def renderizar_placar_geral(df) -> str:
    total = len(df)
    validadas = (df["status"].str.contains("validada em produção", case=False, na=False)).sum()
    com_erro = (df["status"].str.contains("erro|bloquead", case=False, na=False, regex=True)).sum()
    nao_iniciadas = (df["status"].str.contains("não iniciada", case=False, na=False)).sum()
    pct_cobertura = round(100 * validadas / total) if total else 0

    cards = [
        ("Fontes cobertas e validadas", validadas, "#66bb6a"),
        ("Com erro ou bloqueio ativo", com_erro, "#ef5350"),
        ("Ainda não iniciadas", nao_iniciadas, "#ffb300"),
        ("Cobertura geral", f"{pct_cobertura}%", "#5350a0"),
    ]

    html_cards = ""
    for label, valor, cor in cards:
        html_cards += f"""
        <div class="placar-card">
          <div class="placar-valor" style="color:{cor}">{valor}</div>
          <div class="placar-label">{escapar(label)}</div>
        </div>"""

    return f"""
    <div class="section-label">Placar geral</div>
    <div class="placar-grid">{html_cards}
    </div>"""

# COMMAND ----------

# =============================================================================
# Renderização — Cobertura por fonte/setor
# =============================================================================

def renderizar_cobertura(df) -> str:
    df = df.copy()
    df["setor"] = df["setor"].fillna("Sem setor definido")

    cards = ""
    for setor, grupo in df.groupby("setor"):
        linhas = ""
        for _, fonte in grupo.iterrows():
            sinal = status_para_sinal(fonte["status"])
            docs = fonte["docs_capturados"]
            docs_txt = f"{int(docs)} docs" if docs is not None and docs == docs else "—"
            ultima = fonte["ultima_execucao"]
            ultima_txt = ultima.strftime("%d/%m %H:%M") if ultima is not None and str(ultima) != "NaT" else "nunca rodou"
            linhas += f"""
            <div class="sector-topic">
              <span class="signal-dot {sinal}" style="margin-top:5px;"></span>
              <div class="topic-text">
                <strong>{escapar(fonte['nome_fonte'])}</strong> — {escapar(fonte['status'])}<br>
                <span style="color:#8a87ad;font-size:11px;">{docs_txt} · última execução: {ultima_txt}</span>
              </div>
            </div>"""

        cards += f"""
        <div class="sector-card">
          <div class="sector-card-header">
            <div class="sector-name">{escapar(setor)}</div>
          </div>
          <div class="sector-body">{linhas}
          </div>
        </div>"""

    return f"""
    <div class="section-label">Cobertura por fonte / setor</div>
    <div class="sector-grid">{cards}
    </div>"""

# COMMAND ----------

# =============================================================================
# Renderização — Pendências e bloqueios
# =============================================================================

def renderizar_pendencias(df) -> str:
    if df is None or len(df) == 0:
        itens = '<div class="outro-item">Nenhum bloqueio ativo registrado no momento.</div>'
    else:
        itens = ""
        for _, b in df.iterrows():
            itens += f"""
            <div class="outro-item">
              <div>
                <strong>{escapar(b['nome'])}</strong> — {escapar(b['descricao'])}<br>
                <span style="color:#8a87ad;font-size:11px;">
                  Responsável: {escapar(b['responsavel'] or '—')} · Status: {escapar(b['status_ou_reversibilidade'])}
                </span>
              </div>
            </div>"""

    return f"""
    <div class="section-label">Pendências e bloqueios conhecidos</div>
    <div class="outro-list">{itens}
    </div>"""

# COMMAND ----------

# =============================================================================
# Renderização — Últimas atualizações
# =============================================================================

def renderizar_ultimas_atualizacoes(df, limite: int = 10) -> str:
    df = df.copy()
    df = df[df["ultima_execucao"].notna()].sort_values("ultima_execucao", ascending=False).head(limite)

    if len(df) == 0:
        itens = '<div class="outro-item">Nenhuma execução registrada ainda.</div>'
    else:
        itens = ""
        for _, f in df.iterrows():
            sinal = status_para_sinal(f["status"])
            quando = f["ultima_execucao"].strftime("%d/%m %H:%M")
            erro_txt = f" — {escapar(f['ultimo_erro'])}" if f["ultimo_erro"] else ""
            itens += f"""
            <div class="outro-item">
              <span class="signal-dot {sinal}" style="margin-top:4px;"></span>
              <div>
                <span class="date-chip">{quando}</span> —
                <strong>{escapar(f['nome_fonte'])}</strong>: {escapar(f['status'])}{erro_txt}
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

if SECOES["cobertura_fontes"]:
    blocos.append(renderizar_cobertura(df_fontes))

if SECOES["pendencias"]:
    blocos.append(renderizar_pendencias(df_bloqueios))

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
# (mesma paleta, tipografia e componentes de exemplo_resultado_infra.html)
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

  .container {{ max-width: 900px; margin: 0 auto; padding: 32px 24px 60px 24px; }}

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

  /* Sector grid (cobertura) */
  .sector-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 4px; }}
  .sector-card {{
    background: #fff; border: 1px solid #e0ddf8; border-radius: 12px;
    overflow: hidden; display: flex; flex-direction: column;
  }}
  .sector-card-header {{
    padding: 13px 18px 11px 18px; border-bottom: 1px solid #ece9fb;
    display: flex; align-items: center; justify-content: space-between; gap: 10px;
  }}
  .sector-name {{ font-size: 13px; font-weight: 800; color: #1a1855; }}
  .signal-dot {{ width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; display: inline-block; }}
  .signal-high {{ background: #ef5350; }}
  .signal-medium {{ background: #ffb300; }}
  .signal-low {{ background: #66bb6a; }}
  .sector-body {{ padding: 14px 18px 16px 18px; flex: 1; }}
  .sector-topic {{
    padding: 7px 0; border-bottom: 1px dashed #ede9fb;
    display: flex; gap: 9px; align-items: flex-start;
  }}
  .sector-topic:last-child {{ border-bottom: none; padding-bottom: 0; }}
  .topic-text {{ font-size: 12.5px; line-height: 1.6; color: #2c2a4a; }}
  .topic-text strong {{ color: #1a1855; }}

  /* Listas (pendências, últimas atualizações) */
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

  <div class="footer">Gerado automaticamente a partir de desafio_kinea.research.controle_fontes</div>
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
