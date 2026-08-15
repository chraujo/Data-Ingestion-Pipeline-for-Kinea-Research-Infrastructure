# kinea-infra-ingestion

Pipeline automatizado de ingestão de notícias, dados regulatórios e podcasts
do setor de Infraestrutura, alimentando o briefing diário de pesquisa da
Kinea Investimentos.

> Para convenções de código, fluxo de adição de fonte nova e histórico de
> bugs já resolvidos, veja [CLAUDE.md](CLAUDE.md) — este README cobre visão
> geral, arquitetura e "como rodar"; o [RUNBOOK.md](RUNBOOK.md) cobre
> operação do dia a dia e troubleshooting.

## Para quem é este projeto

Serve o **desk de Infraestrutura da Kinea** (energia, saneamento,
transporte, telecom e óleo & gás no recorte regulatório). O produto final é
um e-mail diário (dias úteis) e um resumo semanal (sexta-feira) com os
temas mais relevantes para decisões de investimento no setor, com
justificativa de impacto e exposição de carteira.

O desafio mais amplo (briefing oficial da Kinea) também cobre CRI, CRA e
Special Situations — esses pipelines existem, mas em outro Job/pasta de
workspace (`Ingest-News-Cri`, `/Workspace/Shared/Research_CRI/...`) e estão
fora do escopo deste repositório.

## Arquitetura em alto nível

```
dispatchers (ingestores/*)
      │  cada um escreve .txt (texto) + .json (metadados) por documento
      ▼
Volume: /Volumes/desafio_kinea/research/research_volume/infraestrutura/files/{YYYY-MM-DD}/{SETOR}/
      │
      ├──► Processa_Daily_Infra  ──► e-mail diário (dias úteis)
      │
      └──► Processa_Weekly_Infra ──► Monta-Resumo-Final ──► e-mail semanal (sexta)
                                          (agrega os últimos 7 dias +
                                           relatório de progresso + fontes cobertas)
```

Camada de controle, em paralelo (não faz parte do fluxo de dados, mas cada
dispatcher grava nela ao final da execução):

```
dispatcher → utils_controle.atualizar_status_fonte() → desafio_kinea.research.controle_fontes
```

### O que roda onde (estado real do workspace, conferido em 2026-08-15)

| Job (Databricks) | Agenda (America/Sao_Paulo) | O que faz |
|---|---|---|
| `Ingest-news-INFRA` | 07:45:46 diário | Roda os 12 dispatchers de captura (lista completa no Runbook) |
| `Processa Dailies` (task `Processa_Daily_Infra`) | 09:30:41 diário | Lê o Volume do dia, chama o serviço externo (`ui-agents.azurewebsites.net`), envia o e-mail diário |
| `Relatório de Progresso INFRA` | 09:55:42 diário | Gera o relatório HTML de progresso das fontes |
| `Resumo Semanal INFRA` | sexta-feira 12:00 | `gera_config` → `Relatorio-Progresso` → `Processa-Weekly` → `Monta-Resumo-Final` |

**⚠️ Achado relevante, não um detalhe menor**: o notebook
`Processa_Daily_Infra` — a peça que efetivamente gera e envia o briefing
**diário** — não vive neste repositório. Ele está em
`/Workspace/Users/marcos.markevich@kinea.com.br/Research Desafio/Processa Daily Infra`,
fora do Git folder, e portanto fora de controle de versão, do fluxo de PR e
desta documentação em termos de rastreabilidade de mudança. O equivalente
**semanal** (`Processa_Weekly_Infra.ipynb`), por outro lado, está
devidamente versionado em `scripts/`. Isso não é algo que este trabalho de
documentação mudou ou deveria mudar sozinho — está registrado aqui como um
gap real de arquitetura para quem for planejar a próxima etapa (ver seção
"O que vai precisar de atualização" no fim do Runbook).

## Decisões de design importantes

### Dispatchers parametrizados: genérico vs. notebook próprio

Em vez de um notebook por fonte, fontes com a **mesma arquitetura de
captura** (RSS, scraping HTML, PDF) são agrupadas num dispatcher genérico
único, parametrizado por um widget Databricks `fonte` (default `"todas"`):

- `ingestores/Notebooks unificados/ingest-scraping.ipynb` — scraping HTML
  (WordPress/Elementor/tagDiv/Plone/Next.js SSR), 14 fontes num único
  notebook via `CONFIGS_FONTES`, cada entrada com sua função `listar_*` e,
  quando necessário, `extrair_data`/`extrair_titulo` próprios.
- `ingestores/Notebooks unificados/ingest-PDF.ipynb` — listagens que
  apontam para PDFs (download + `pypdf` para extração de texto).
- `ingestores/Notebooks unificados/ingest-news-rss-infra.ipynb` — feeds RSS
  padrão.
- `ingestores/Notebooks unificados/ingest-news-site-inteiro.ipynb` — sites
  que exigem raspar a página inteira (Estadão, O Globo, Valor-Infra,
  Moody's, PPI).

Fontes que **não** se encaixam nesse molde — porque exigem Selenium/
Playwright (Brazil Journal), transcrição de áudio (MinutoMega), engenharia
reversa de API (ONS), ou paginação/estrutura muito particular (Diário
Oficial de MS, DOE-SP/SPI, DOE-PA/ARTRAN) — ganham **notebook próprio** em
`ingestores/{SETOR}/`. A decisão de qual caminho seguir é a Fase 2 do fluxo
de adição de fonte (ver CLAUDE.md e RUNBOOK.md).

Esse desenho existe para não precisar de N notebooks quase-idênticos: um
dispatcher genérico compartilha `baixar_pagina()` (retry com `httpx` +
`curl_cffi`/impersonation de TLS para fontes atrás de WAF/bot-management),
`extrair_texto_generico()` (cascata de seletores CSS conhecidos +
fallback para `<article>`), deduplicação por manifesto (`.json` de URLs já
processadas) e a chamada final a `atualizar_status_fonte()` — só a lógica
específica de listagem/data/título de cada fonte muda.

### Determinístico vs. LLM, onde precisão importa

A extração de conteúdo (o que vira `.txt`/`.json` no Volume) é
**inteiramente determinística** — scraping com seletores CSS explícitos,
regex para datas, sem nenhuma chamada a LLM. Isso é proposital: a etapa que
decide "isso é notícia de verdade, com título/data/texto corretos" precisa
ser auditável e reproduzível, não sujeita a alucinação. Os únicos pontos
onde LLM entra são a jusante, já em `scripts/gera_config.ipynb` (seleção
de relevância, clusterização por tema, sumarização, formatação HTML) — ver
[docs/prompts/README.md](docs/prompts/README.md).

### Estrutura de setores

Cada documento capturado é salvo em
`files/{data}/{SETOR}/arquivo.txt` (+ `.json` de metadados), onde `SETOR`
é um dos seguintes (usado tanto na pasta física quanto no campo
`**Setor:**` do briefing final): `ENERGIA`, `SANEAMENTO`, `TRANSPORTE`,
`TELECOM`, `REGULATORIO`, `GERAL`. O código de leitura do lado do
`Processa_Daily_Infra`/`Processa_Weekly_Infra` percorre essa estrutura com
`os.walk` (não `os.listdir` — ver Runbook, é um bug já corrigido) para
funcionar independente de quais subpastas de setor existem num dia
específico.

## Como rodar cada peça principal

Pré-requisitos: perfil `kinea-desafio` configurado no Databricks CLI, Git
folder do Databricks linkado a este repo (ver CLAUDE.md para IDs e
caminhos exatos).

**Rodar a captura de um dispatcher específico** (útil ao testar uma fonte
nova, ou re-rodar um dia com falha):

```bash
databricks jobs run-now --json '{"job_id": 1059728460076257, "only": ["Ingest-scraping"]}' --profile kinea-desafio
```

Troque `"Ingest-scraping"` pelo `task_key` do dispatcher desejado (veja a
tabela completa no Runbook) — ou rode o notebook direto pelo Workspace UI
com o widget `fonte` apontando para uma fonte específica em vez de
`"todas"`.

**Rodar o pipeline diário completo** (captura → briefing → e-mail):

```bash
databricks jobs run-now --job-id 1059728460076257 --profile kinea-desafio   # captura
databricks jobs run-now --job-id 392719769937284 --profile kinea-desafio   # Processa Dailies (Daily Infra + Daily CRI)
```

**Rodar o pipeline semanal completo**:

```bash
databricks jobs run-now --job-id 895989585317993 --profile kinea-desafio
```

**Adicionar uma fonte nova**: siga o fluxo de 3 fases documentado em
[CLAUDE.md](CLAUDE.md#como-adicionar-uma-fonte-nova) e formalizado com
passo a passo em [RUNBOOK.md](RUNBOOK.md).

**Corrigir dados da tabela de controle** ou **gerar o relatório de
progresso manualmente**: ver seção correspondente no Runbook.

## Sincronização Git ↔ Databricks

Não é automática — cada lado precisa de uma ação manual depois de editar
localmente:

```bash
git push
databricks repos update <ID-do-repo> --branch main --profile kinea-desafio
```

Detalhes (IDs, caminhos, recuperação de conflito) em [CLAUDE.md](CLAUDE.md).
