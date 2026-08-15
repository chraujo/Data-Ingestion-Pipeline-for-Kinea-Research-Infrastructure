# Runbook Operacional — kinea-infra-ingestion

Guia de operação do dia a dia: como adicionar uma fonte nova, o que fazer
quando algo quebra, e como rodar as peças manuais do pipeline. Para
arquitetura e decisões de design, veja [README.md](README.md); para
convenções de código e histórico completo de bugs já resolvidos, veja
[CLAUDE.md](CLAUDE.md) (este Runbook formaliza o fluxo de 3 fases que já
estava descrito lá, e adiciona uma seção de troubleshooting operacional
mais ampla).

## Fluxo de 3 fases para adicionar fonte nova

Sempre nessa ordem, sem pular etapa. (Fonte: CLAUDE.md, seção "Como
adicionar uma fonte nova" — reproduzido aqui em formato de checklist.)

### Fase 1 — Teste isolado

- [ ] Cria `ingestores/[SETOR]/teste_[nome_fonte].ipynb`
- [ ] Sem dependência de nenhum dispatcher existente, sem
      `atualizar_status_fonte`, sem gravar nada em produção
- [ ] Confirma antes de assumir: não presume WAF, paginação, paywall,
      posição da data (listagem vs. página individual), ou formato de
      site — testa e reporta o que encontrou
- [ ] Valida contra o site real (não só leitura de HTML salvo em disco)
- [ ] Notebook é descartável — não precisa seguir padrão de produção

### Fase 2 — Avaliação e integração no dispatcher genérico ou notebook próprio

Encaixa num dispatcher genérico (`ingest-news-rss-infra`,
`ingest-scraping`, `ingest-PDF`) se: "baixar uma URL e extrair itens de
forma padrão" — sem navegador, sem transcrição de áudio, sem parsing
muito particular daquele site.

Precisa de notebook próprio se: exige Selenium/Playwright, processamento
de áudio, ou lógica de descoberta/parsing que não se resume a "baixar e
extrair".

- [ ] Antes de editar um dispatcher compartilhado, **releia o arquivo do
      zero** (`git diff --stat` contra o HEAD anterior que você leu) —
      outras sessões/pessoas podem ter adicionado fontes em paralelo;
      editar em cima de uma leitura desatualizada já causou perda de
      conteúdo de outras fontes neste projeto (ver Troubleshooting)
- [ ] Adiciona a função `listar_*()` (e `extrair_data`/`extrair_titulo`
      próprios, se a listagem não trouxer isso pronto)
- [ ] Adiciona a entrada em `CONFIGS_FONTES`
- [ ] Valida localmente rodando o dispatcher inteiro (não só a função
      nova) contra o site real, com o widget `fonte` apontando só para a
      fonte nova
- [ ] Commit + push + `databricks repos update`

### Fase 3 — Registro em `controle_fontes`

- [ ] **Confirma se já existe uma linha placeholder** para essa fonte no
      catálogo original (nome exato, não aproximado) antes de decidir
      entre UPDATE e INSERT
- [ ] Se existir: UPDATE preenchendo `source_id`, `status = 'Não
      iniciada'`, `metodo_captura`, `notebooks_responsaveis`,
      `tasks_responsaveis`, `notas` — **na própria linha**, sem duplicar
- [ ] Se não existir com esse nome exato: confirma com quem pediu antes
      de criar uma linha nova (INSERT)
- [ ] Confirma que o `source_id` escolhido não colide com nenhum
      existente:
      ```sql
      SELECT source_id, COUNT(*) FROM desafio_kinea.research.controle_fontes
      GROUP BY source_id HAVING COUNT(*) > 1;
      ```
- [ ] Define a `importância` (Alta/Média/Baixa) — não deixa em branco.
      Se o catálogo original já tinha um valor coerente, preserva-o em
      vez de reconferir do zero
- [ ] Atualiza `fontes_cobertas` na linha correspondente de
      `controle_notebooks`
- [ ] Roda a fonte em produção de fato (não só localmente) e confirma
      `status = 'Coberta e validada em produção'` com `docs_capturados >
      0` antes de considerar a fase concluída

## Troubleshooting

### `%run utils_controle` executado antes do `restartPython()` apaga o namespace

**Sintoma**: `NameError: name 'atualizar_status_fonte' is not defined`
(ou comportamento equivalente onde a função parece "sumir") em algum
ponto do notebook, mesmo com o `%run` presente.

**Causa**: `dbutils.library.restartPython()` reinicia o processo Python
do notebook e apaga qualquer coisa que um `%run` anterior tenha injetado
no namespace. Se a célula de `%run "...utils_controle"` vier antes do
`restartPython()` (por exemplo, logo depois do `%pip install`, antes do
restart), o restart apaga o que acabou de ser carregado.

**Correção**: ordem fixa de células, sempre: `%pip install` →
`dbutils.library.restartPython()` → `%run ".../utils_controle"` → resto
do notebook. Corrigido no PPI (commit `00b4243`, "Corrige ordem: %run
utils_controle depois do restartPython (PPI)"); ver também CLAUDE.md,
item 1 da lista de bugs conhecidos.

### `os.listdir()` não encontra os documentos do dia (estrutura por setor)

**Sintoma**: `Processa_Daily_Infra`/`Processa_Weekly_Infra` reporta zero
documentos encontrados (ou bem menos que o esperado) para um dia em que
os dispatchers claramente rodaram e salvaram arquivos.

**Causa**: os documentos passaram a ficar em subpastas por setor
(`files/{data}/ENERGIA/`, `.../SANEAMENTO/`, etc.) em vez de soltos
direto na pasta do dia. Uma função que lê com `os.listdir(date_dir)` só
vê o nível raiz — não desce nas subpastas de setor — e portanto não
enxerga nenhum arquivo.

**Correção**: usar `os.walk(date_dir)` em vez de `os.listdir()`,
coletando todo `.json` em qualquer profundidade. Já corrigido em
`Processa_Daily_Infra` e `Processa_Weekly_Infra.ipynb` (`scripts/`) — ver
o comentário explicativo em ambos os arquivos, próximo à função
`load_documents_from_volume`. Cobre tanto a estrutura nova (por setor)
quanto a antiga (arquivos soltos na raiz), sem precisar saber os nomes
dos setores de antemão.

### Variável de ambiente ausente (`AZURE_STORAGE_CONNECTION_STRING` / `URL_EMAIL` / `UI_AGENTS_DEV_API_KEY`)

**Sintoma**: `KeyError: 'AZURE_STORAGE_CONNECTION_STRING'` (ou
`'UI_AGENTS_DEV_API_KEY'`, ou `'URL_EMAIL'`) logo nas primeiras células
de `Processa_Daily_Infra` ou `Processa_Weekly_Infra.ipynb` — o notebook
lê essas três variáveis direto via `os.environ['NOME']` (sem `.get()`
com fallback), então qualquer uma ausente quebra imediatamente, antes de
qualquer chamada de rede acontecer.

**Causa**: essas três variáveis (string de conexão do Azure Storage, URL
do endpoint interno de envio de e-mail, e a chave de API do serviço
`ui-agents.azurewebsites.net`) são configuradas como variáveis de
ambiente do cluster Databricks, não como segredo do notebook em si — se
o cluster for recriado, trocado, ou se essas variáveis forem definidas
só num cluster específico, um notebook rodando em outro cluster (ou um
cluster novo) não as encontra.

**Correção**: confirmar que o cluster usado tem as três variáveis
configuradas (Compute → cluster → Advanced options → Environment
variables), replicando de um cluster que já funciona se necessário. Não
há fallback de código para isso — é puramente configuração de
infraestrutura do cluster.

### Erro de permissão `Manage` ao instalar biblioteca em cluster compartilhado

> ⚠️ **Detalhes a confirmar.** Este item foi apontado como problema já
> resolvido no projeto, mas não encontrei evidência dele no histórico de
> commits, no `MENSAGENS.md`, nem no histórico de execuções de Job que
> consegui consultar via CLI no momento em que este Runbook foi escrito
> (2026-08-15). Sintoma, causa e correção precisam ser preenchidos por
> quem viveu o incidente antes desta seção ser considerada confiável.

### Erro 401 `AuthorizationFailed` (IP allowlist) no envio de e-mail

> ⚠️ **Detalhes a confirmar.** Mesma situação do item acima — apontado
> como problema real já resolvido, mas sem evidência localizável no
> repositório ou no histórico de execuções consultado. Os dois erros que
> de fato aparecem no histórico de runs falhados de `Processa_Daily_Infra`
> são `422 Client Error: Unprocessable Entity` no POST para
> `/report_agent/start_report` (validação de payload do serviço externo,
> não IP allowlist) — se o 401 aconteceu, foi antes da janela de
> histórico disponível ou num contexto que não consultei. Preencher
> sintoma exato (mensagem de erro completa), onde no fluxo acontece
> (envio de e-mail via `URL_EMAIL`, ou outra chamada), e como foi
> corrigido.

## Rodando a correção de dados do relatório

`scripts/corrigir_dados_relatorio.py` é um script de correção pontual,
já executado — trata três problemas específicos e documentados no próprio
cabeçalho do arquivo (status desatualizado da Megawhat, cobertura indireta
da AGEMS via Diário Oficial de MS, agrupamento visual das variações de
captura da AGESAN — Resoluções/CSR/DC). Também funde linhas "fantasma" do
catálogo original que ficaram duplicadas com a linha real criada quando a
fonte foi implementada, e resolve pendências de `importancia_original =
'N/D'` herdando a prioridade do órgão-pai.

Seguro rodar de novo — usa `ADD COLUMN IF NOT EXISTS`-equivalente (ver
nota abaixo) e `UPDATE`s idempotentes. A primeira célula é só diagnóstico
(`display`, sem alterar nada) — leia antes de deixar rodar até o fim.

```bash
databricks jobs submit --json '{
  "run_name": "corrigir_dados_relatorio",
  "tasks": [{
    "task_key": "corrigir",
    "existing_cluster_id": "<CLUSTER_ID>",
    "notebook_task": {"notebook_path": "/Workspace/Shared/Research_Infra/Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure/scripts/corrigir_dados_relatorio"}
  }]
}' --profile kinea-desafio
```

> **Nota sobre duas cópias quase idênticas**: existem
> `scripts/corrigir_dados_relatorio.py` e `scripts/corrigir_dados_relatorio
> (2).py` no repositório — a segunda envolve os `ALTER TABLE ADD COLUMNS`
> em `try/except` para não quebrar se a coluna já existir (mais segura
> para rodar de novo num ambiente onde já rodou antes). Nenhuma das duas
> foi apagada ou consolidada como parte deste trabalho de documentação —
> ver "O que vai precisar de atualização" no fim deste documento.

## Rodando o relatório de progresso manualmente

O relatório de progresso é gerado por `scripts/gerar_relatorio_progresso_B.py`
(a versão "B" — seções empilhadas, funciona em e-mail; existe também uma
versão "A" com abas clicáveis em JavaScript, que **não** é a usada em
produção — ver nota abaixo). Lê das tabelas `controle_*` mais o arquivo
[MENSAGENS.md](MENSAGENS.md) (anotações manuais por seção, editável
direto no repo sem precisar de Databricks/SQL).

```bash
databricks jobs run-now --job-id 313188186125095 --profile kinea-desafio
```

> **Nota sobre "A" vs "B"**: `gerar_relatorio_progresso_A.py` usa abas
> clicáveis (`<button onclick="mostrarAba(...)">`) — isso não funciona em
> clientes de e-mail (sem JavaScript). `gerar_relatorio_progresso_B.py` é
> a versão corrigida (seções empilhadas, mesmo raciocínio de
> `SYSTEM_FORMATTER_PROMPT` vs. o antigo formato de abas em
> `gera_config.ipynb`) e é a que a task `Progresso-INFRA`/`Relatorio-
> Progresso` de fato executa. Existe ainda um terceiro arquivo,
> `gerar_relatorio_progresso.py` (sem sufixo), não referenciado por
> nenhum Job — provavelmente uma versão anterior às duas acima.

## Jobs agendados (referência rápida)

| Job | job_id | Agenda (America/Sao_Paulo) | Tasks |
|---|---|---|---|
| `Ingest-news-INFRA` | `1059728460076257` | 07:45:46 diário | `Ingest-AGEMS`, `Ingest-InfraJournal`, `Ingest-MinutoMega`, `Ingest-ONS`, `Ingest-PDF`, `Ingest-PPI`, `Ingest-Pipeline-oglobo`, `Ingest-SPI`, `Ingest-google-news`, `Ingest-rss-unificado`, `Ingest-scraping`, `Ingest-ARTRAN` |
| `Processa Dailies` | `392719769937284` | 09:30:41 diário | `Processa_Daily_CRI`, `Processa_Daily_Infra` (fora do repo — ver README) |
| `Relatório de Progresso INFRA` | `313188186125095` | 09:55:42 diário | `Progresso-INFRA` (`gerar_relatorio_progresso_B`) |
| `Resumo Semanal INFRA` | `895989585317993` | sexta-feira 12:00 | `gera-config` → `Relatorio-Progresso` → `Processa-Weekly` → `Monta-Resumo-Final` |
| `Ingest-News-Cri` | `134256360815977` | — | Fora de escopo (pipeline de CRI, pasta `Research_CRI` separada) |

Para rodar só uma task específica de um Job (isolando das demais):

```bash
databricks jobs run-now --json '{"job_id": <JOB_ID>, "only": ["<TASK_KEY>"]}' --profile kinea-desafio
```

## O que vai precisar de atualização

Este Runbook (e o README/docs de prompts que o acompanham) reflete o
estado do repositório e do workspace em **2026-08-15**. Partes que vão
ficar desatualizadas conforme o projeto avança:

- **Tabela de fontes/dispatchers no README e neste Runbook** — hoje há 87
  fontes catalogadas (34 "Coberta e validada em produção", 38 "Não
  iniciada", 9 "Descartada", 3 "Bloqueada", 2 "Coberta indiretamente", 1
  "Iniciada — não validada"). Esse número muda a cada fonte nova
  integrada — não tratar os números aqui como atuais além do momento em
  que foram escritos; conferir direto em `controle_fontes` antes de citar
  em qualquer material externo.
- **As duas seções de troubleshooting marcadas como "detalhes a
  confirmar"** (permissão `Manage`, 401 IP allowlist) precisam ser
  preenchidas por quem viveu esses incidentes.
- **`Processa_Daily_Infra` fora do Git folder** — se/quando esse notebook
  for movido para dentro do repositório (recomendável, dado que é a peça
  que efetivamente gera o briefing diário), toda referência a "fora do
  repo" neste Runbook e no README precisa ser removida e substituída pelo
  caminho real dentro de `scripts/` ou onde for parar.
- **Duplicatas não resolvidas**: `gerar_relatorio_progresso.py`/`_A.py`/
  `_B.py`; `corrigir_dados_relatorio.py` vs. `corrigir_dados_relatorio
  (2).py`. (A duplicata equivalente do `gera_config.ipynb` na raiz já foi
  removida em 2026-08-14, depois deste levantamento ter começado — ver
  `docs/prompts/README.md`.) Se alguém consolidar/apagar as demais cópias,
  as notas específicas sobre elas neste documento ficam obsoletas e devem
  ser removidas.
- **`ANTAQ` não aparece em `controle_notebooks.fontes_cobertas` para
  `ingest-scraping-generico`**, apesar de estar de fato na
  `CONFIGS_FONTES` do dispatcher (confirmado lendo o notebook
  diretamente) — parece um passo de Fase 3 não concluído por quem
  adicionou essa fonte. Vale conferir e corrigir quando alguém for mexer
  nessa fonte de novo.
- **Pipeline de NLP/enriquecimento (`gera_config.ipynb` e a cadeia de
  prompts)** — este Runbook documenta a arquitetura e onde as coisas
  vivem, não a qualidade ou maturidade dos prompts em si. Conforme essa
  peça evoluir (novos campos de saída, novos critérios de seleção,
  mudança no formato do e-mail), a seção correspondente em
  [docs/prompts/README.md](docs/prompts/README.md) precisa ser revisada
  junto — ela descreve *onde* os prompts vivem, não substitui a leitura
  do `gera_config.ipynb` real.
- O comando de sincronização no README usa um placeholder (`<ID-do-repo>`)
  em vez do ID literal do Git folder do Databricks — de propósito, porque
  esse ID já mudou uma vez neste projeto (incidente de exclusão acidental
  da pasta do workspace) e o valor atual (`1430318475591826`, já
  corrigido em CLAUDE.md) pode mudar de novo. Consultar CLAUDE.md para o
  ID vigente em vez de confiar num valor fixo aqui.
