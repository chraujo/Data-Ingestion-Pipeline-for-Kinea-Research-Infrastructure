# Databricks notebook source
# =============================================================================
# montar_resumo_semanal_completo.py
#
# Roda toda SEXTA, LOGO DEPOIS do Processa_Weekly_Infra.ipynb. Junta:
#   1. O resumo semanal (gerado por LLM: itens da semana + Conclusão da
#      Semana, já incluindo o ranking de fontes mais relevantes) --
#      arquivo salvo por Processa_Weekly_Infra.ipynb
#   2. Uma seção de "Fontes já cobertas em produção" (código nosso,
#      determinístico -- consulta direta em controle_fontes, agrupada por
#      importância), com link pro relatório de progresso completo
#      (reaproveita o link já publicado no Blob Storage por
#      gerar_relatorio_progresso.py -- não gera um link novo)
#   3. Um botão de feedback por e-mail, igual ao do briefing diário
#
# Ordem final do documento: Resumo semanal -> Conclusão da Semana (com o
# ranking de fontes) -> Fontes já cobertas -> botão de feedback.
#
# Envia por e-mail (mesmo mecanismo de sempre, URL_EMAIL), com o CSS já
# tratado pelo premailer (mesma correção aplicada ao briefing diário --
# sem isso, o e-mail sai com o CSS aparecendo como texto).
# =============================================================================

# COMMAND ----------

# MAGIC %pip install --quiet premailer
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import json
import requests
from datetime import datetime, timezone

CATALOGO = "desafio_kinea"
SCHEMA = "research"
PREFIXO = "controle_"

BASE_VOLUME_PATH = "/Volumes/desafio_kinea/research/research_volume/infraestrutura"
OUTPUTS_WEEKLY_ROOT = os.path.join(BASE_VOLUME_PATH, "outputs_weekly")
PASTA_RELATORIOS = "/Volumes/desafio_kinea/research/research_volume/relatorios"

CAMINHO_RESUMO_BRUTO = os.path.join(OUTPUTS_WEEKLY_ROOT, "resumo_semanal_bruto.html")
CAMINHO_PONTEIRO_LINK = os.path.join(PASTA_RELATORIOS, "ultimo_link_relatorio.json")

AGORA = datetime.now(timezone.utc).astimezone()
AGORA_ISO = AGORA.strftime("%Y-%m-%d")
AGORA_LEGIVEL = AGORA.strftime("%d/%m/%Y às %H:%M")

# COMMAND ----------

# =============================================================================
# 1) Carrega o resumo semanal bruto (gerado pelo Processa_Weekly_Infra.ipynb)
# =============================================================================

if not os.path.exists(CAMINHO_RESUMO_BRUTO):
    raise FileNotFoundError(
        f"Resumo semanal não encontrado em {CAMINHO_RESUMO_BRUTO} -- "
        f"rode o Processa_Weekly_Infra.ipynb primeiro."
    )

with open(CAMINHO_RESUMO_BRUTO, "r", encoding="utf-8") as f:
    html_resumo = f.read()

print(f"[ok] Resumo semanal carregado ({len(html_resumo)} chars)")

# COMMAND ----------

# =============================================================================
# 2) Fontes já cobertas em produção, agrupadas por importância (query
#    determinística, direto na tabela de controle -- não passa pelo LLM)
# =============================================================================

def escapar(texto) -> str:
    if texto is None:
        return ""
    texto = str(texto)
    return (texto.replace("&", "&amp;").replace("<", "&lt;")
                 .replace(">", "&gt;").replace('"', "&quot;"))


df_fontes = spark.table(f"{CATALOGO}.{SCHEMA}.{PREFIXO}fontes").toPandas()
cobertas = df_fontes[df_fontes["status"] == "Coberta e validada em produção"].copy()

ORDEM_IMPORTANCIA = ["Alta", "Média", "Media", "Baixa"]
blocos_importancia = []
for nivel in ORDEM_IMPORTANCIA:
    subset = cobertas[cobertas["importancia_original"] == nivel]
    if len(subset) == 0:
        continue
    itens = "".join(f"<li>{escapar(nome)}</li>" for nome in sorted(subset["nome_fonte"].tolist()))
    blocos_importancia.append(f'<h4>{escapar(nivel)} ({len(subset)})</h4><ul class="fontes-condensado">{itens}</ul>')

html_fontes_por_importancia = "".join(blocos_importancia) if blocos_importancia else "<p>Nenhuma fonte coberta registrada.</p>"

print(f"[ok] {len(cobertas)} fontes cobertas encontradas, agrupadas em {len(blocos_importancia)} faixa(s) de importância")

# COMMAND ----------

# =============================================================================
# 3) Link pro relatório de progresso completo (reaproveita o já publicado
#    por gerar_relatorio_progresso.py -- não gera um link novo aqui)
# =============================================================================

link_relatorio_completo = None
if os.path.exists(CAMINHO_PONTEIRO_LINK):
    try:
        ponteiro = json.load(open(CAMINHO_PONTEIRO_LINK, encoding="utf-8"))
        link_relatorio_completo = ponteiro.get("link")
        print(f"[ok] Link do relatório de progresso: {link_relatorio_completo} (gerado em {ponteiro.get('gerado_em')})")
    except Exception as e:
        print(f"[aviso] Não foi possível ler o ponteiro de link: {e}")
else:
    print(f"[aviso] Ponteiro de link não encontrado em {CAMINHO_PONTEIRO_LINK} -- "
          f"rode gerar_relatorio_progresso.py pelo menos uma vez antes.")

link_html = (
    f'<a href="{link_relatorio_completo}" class="link-relatorio-completo" target="_blank">Ver relatório de progresso completo →</a>'
    if link_relatorio_completo else
    '<p class="texto-fraco">(Link do relatório de progresso completo indisponível nesta semana.)</p>'
)

# COMMAND ----------

# =============================================================================
# 4) Monta a seção de fontes cobertas + botão de feedback, e insere no
#    HTML do resumo semanal (antes do </body> -- funciona independente da
#    estrutura exata que o serviço externo usou por fora).
# =============================================================================

EMAILS_FEEDBACK = [
    "chrisaraujofsz@gmail.com",
    "belagiusti@gmail.com",
    "marcos.markevich@kinea.com.br",
]

EMAILS_RESUMO_SEMANAL = EMAILS_FEEDBACK + [
    "bruno.pedra@kinea.com.br",
    "maria.mochinski@kinea.com.br",
]

_assunto_feedback = (
    f"Feedback%20sobre%20o%20Resumo%20Semanal%20de%20Infraestrutura%20-%20{AGORA_ISO.replace('-', '%2F')}"
)
_mailto_feedback = f"mailto:{','.join(EMAILS_FEEDBACK)}?subject={_assunto_feedback}"

secao_final = f"""
<div class="fontes-cobertas-section">
  <h2>Fontes Já Cobertas em Produção</h2>
  {html_fontes_por_importancia}
  {link_html}
</div>

<div class="feedback-box">
  <a class="feedback-link" href="{_mailto_feedback}">Dar feedback sobre o Resumo Semanal</a>
</div>
"""

if "</body>" in html_resumo:
    html_completo = html_resumo.replace("</body>", secao_final + "</body>")
else:
    # Caso o HTML retornado não tenha </body> por algum motivo -- não
    # descarta a seção, só anexa no final mesmo.
    html_completo = html_resumo + secao_final

print("[ok] Seção de fontes cobertas + botão de feedback inseridos")

# COMMAND ----------

# =============================================================================
# 5) Trata o CSS pro formato de e-mail (mesma correção do briefing diário
#    -- sem isso, o <style> pode aparecer como texto em vez de aplicado)
# =============================================================================

from premailer import transform

html_para_email = transform(html_completo)

# COMMAND ----------

# =============================================================================
# 6) Salva local (backup) e envia por e-mail
# =============================================================================

os.makedirs(OUTPUTS_WEEKLY_ROOT, exist_ok=True)
caminho_completo_local = os.path.join(OUTPUTS_WEEKLY_ROOT, f"resumo_semanal_completo_{AGORA_ISO}.html")
with open(caminho_completo_local, "w", encoding="utf-8") as f:
    f.write(html_completo)
print(f"[ok] Cópia local salva em: {caminho_completo_local}")

ENVIAR_EMAIL = True

if ENVIAR_EMAIL:
    for _email in EMAILS_RESUMO_SEMANAL:
        _payload = json.dumps({
            "body": html_para_email,
            "to": _email,
            "subject": f"Resumo Semanal - Infraestrutura - {AGORA_ISO}",
        })
        _headers = {"Content-Type": "application/json"}
        _url_email = os.environ["URL_EMAIL"]
        _resposta = requests.request("POST", _url_email, headers=_headers, data=_payload, allow_redirects=False)
        print(f"[email] enviado para {_email}: status {_resposta.status_code}")
else:
    print("[email] ENVIAR_EMAIL=False -- pulado")
