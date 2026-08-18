# CLAUDE.md — Data Ingestion Pipeline (Kinea Research / Infraestrutura)

Contexto persistente do projeto para sessões do Claude Code. Leia isto antes de
propor mudanças na arquitetura, nomes de tabelas, ou fluxo de sincronização.

## O que é este projeto

Pipeline de ingestão de notícias/documentos regulatórios (setor de
infraestrutura — energia, saneamento, transporte, regulatório) para a Kinea
Research. Roda em Databricks (Azure), workspace `wks-adb-desafio-kna`.

Escopo mais amplo do desafio (briefing oficial): cobre também CRI, CRA e
Special Situations, com catálogo de 79 fontes — mas o trabalho atual está
concentrado na parte de Infraestrutura, que já tinha pipeline funcionando
antes do briefing formal chegar.

## Infraestrutura de acesso (já configurada, não precisa refazer)

- **Databricks CLI**: profile `kinea-desafio`, host
  `https://adb-6915425056083185.5.azuredatabricks.net`
- **GitHub CLI**: autenticada via `gh auth login`, usuário `chraujo`
- **Repositório**: `https://github.com/chraujo/Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure`
- **Git folder no Databricks**: já linkado ao mesmo repo acima.
  - ID: `1430318475591826`
  - Caminho no Workspace: `/Workspace/Shared/Research_Infra/Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure`

### Fluxo de sincronização (não é automático — cada lado precisa de ação manual)

```
edita local (VS Code) → git push → databricks repos update 1430318475591826 --branch main --profile kinea-desafio
```

Sem o `repos update`, o Git folder no Databricks **não** reflete o push sozinho.
Se alguém editar direto no notebook do navegador sem commitar, o próximo
`repos update` local vai dar erro de conflito — nesse caso, descartar a
edição solta no painel do Git folder do Databricks antes de tentar de novo.

## Unity Catalog — onde os dados vivem

- **Catálogo**: `desafio_kinea`
- **Schema de conteúdo**: `research` (Volume `research_volume`, onde ficam os
  artefatos `.txt`/`.json` extraídos)
- **Schema de controle**: também `research` (não existe schema `controle`
  dedicado — o usuário atual não tem privilégio `CREATE SCHEMA` no catálogo,
  só escrita dentro de schemas já existentes). As tabelas de controle usam
  prefixo `controle_` em vez de schema separado:
  - `desafio_kinea.research.controle_fontes`
  - `desafio_kinea.research.controle_notebooks`
  - `desafio_kinea.research.controle_tasks`
  - `desafio_kinea.research.controle_bloqueios`
  - `desafio_kinea.research.controle_changelog_historico`

Se algum dia um admin liberar `CREATE SCHEMA`, dá pra migrar para um schema
`controle` dedicado — não é urgente.

## `controle_fontes` — a tabela de status ao vivo

Essa é a peça central da Etapa 3 do projeto: os dispatchers atualizam essa
tabela a cada execução, então ela reflete a realidade (não é mais mantida à
mão como era no Excel).

**Regra de ouro: `source_id` precisa ser único de verdade.** Já corrigimos um
bug real onde 5 fontes diferentes (Valor/Pipeline, Estadão, O Globo, PPI,
Moody's) compartilhavam o `source_id` literal `"site_page"` — copiado do
mesmo template sem ser customizado. Isso faria o `MERGE INTO` atualizar todas
de uma vez. Ao criar uma fonte nova ou notebook novo, sempre conferir se o
`source_id` escolhido já não existe:

```sql
SELECT source_id, COUNT(*) FROM desafio_kinea.research.controle_fontes
GROUP BY source_id HAVING COUNT(*) > 1;
```

**Pendência conhecida, não resolvida ainda**: as 3 variações de Resoluções da
AGESAN-RS (`agesan_rs_resolucoes`, `_csr`, `_dc`) estão registradas como uma
única linha na tabela, com os 3 IDs concatenados numa string só
(herdado assim do Excel original). O `MERGE INTO` do dispatcher de PDF não
encontra match para elas — precisa separar em 3 linhas distintas.

**Pendência conhecida, não resolvida ainda**: ~25 Resoluções da ARPE
(`arpe_resolucoes`) — a maioria das mais recentes de 2026, e algumas antigas
esparsas — não têm link `.pdf` direto na listagem
(`/legislacao/resolucoes-arpe`); o link vai para uma página HTML própria
(`/resolucao-arpe-n-XXX`) com o texto integral embutido, sem PDF associado.
Isso não se encaixa no contrato de `processar_pdf()` (baixa bytes, extrai via
`pypdf`) — essas resoluções ficam de fora da captura por ora, contadas no log
do dispatcher como "ignoradas (sem PDF direto)". As ~300 restantes (com PDF
direto, cobrindo o histórico completo desde 2001) já são capturadas
normalmente via `listar_arpe_resolucoes()` em `ingest-PDF.ipynb`.

## Convenção: como um dispatcher atualiza `controle_fontes`

Toda fonte, ao final do processamento (sucesso ou falha), deve chamar a
função compartilhada `atualizar_status_fonte`, definida em
`scripts/utils_controle.py`:

```python
# Célula logo depois do %pip install + dbutils.library.restartPython()
# (NUNCA antes do restart — restart apaga tudo que %run já tiver carregado)
%run "/Workspace/Shared/Research_Infra/Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure/scripts/utils_controle"
```

```python
atualizar_status_fonte(source_id=SOURCE_ID, sucesso=True, docs_capturados=N)
# ou, no except:
atualizar_status_fonte(source_id=SOURCE_ID, sucesso=False, docs_capturados=0, erro=str(e))
```

Cobrir **todos** os desfechos possíveis do dispatcher (sucesso, cada tipo de
falha esperada, exceção genérica) — não só o caminho feliz. Ver
`ingest-diario-oficial-ms.ipynb` como exemplo de notebook com múltiplos
desfechos todos cobertos.

## Bugs já encontrados e corrigidos nessa etapa (não repetir)

1. **Ordem de célula com `%run` antes do `restartPython()`** (PPI): o
   restart apaga o namespace carregado pelo `%run`. Sempre `%pip install` →
   `restartPython()` → `%run utils_controle` → resto do notebook, nessa
   ordem.
2. **`spark` não resolvido dentro de função chamada via `%run`**: resolvido
   em `utils_controle.py` pegando a sessão explicitamente
   (`SparkSession.builder.getOrCreate()`) em vez de depender da injeção
   implícita de `spark` no namespace do notebook.
3. **`selenium` não estava no `%pip install`** do notebook do Brazil
   Journal, apesar do código importar `from selenium import webdriver` —
   `ModuleNotFoundError` que quebrava o notebook inteiro antes mesmo de
   chegar no código instrumentado.
4. **`.mode("errorifexists")`** não é reconhecido neste runtime — usar
   `.mode("error")` (mesmo efeito, nome diferente).
5. **Colunas 100% nulas na migração inicial** (`ultima_execucao`,
   `docs_capturados`, `ultimo_erro`) viravam `NullType` no Spark — corrigido
   forçando tipo explícito via `.cast(...)` na escrita.
6. **`PRIMARY KEY` no Unity Catalog exige `NOT NULL` explícito** na coluna
   antes — `ALTER TABLE ... ALTER COLUMN x SET NOT NULL` antes do
   `ADD CONSTRAINT`.

## Bloqueios conhecidos (não são bugs nossos, é infraestrutura fora do alcance)

- **Selenium/Playwright** (Brazil Journal, PPI): dependem de Chrome em
  `/tmp/chrome/chrome-linux64/chrome`, que só existe se o init script
  `init_selenium2_desafio.sh` estiver anexado ao cluster — isso exige
  permissão de admin do workspace, fora do acesso atual. Esperado que esses
  dois apareçam com `status = 'Erro'` até isso ser resolvido por um admin.
- **AGENERSA**: bloqueada por `robots.txt`, não implementar scraping.
- **ANAC** (`gov.br/anac`): bloqueada por proteção anti-bot ativa (F5/Shape,
  cookie `TSPD`) — mesmo uma requisição HTTP simples e isolada recebe página
  de CAPTCHA em vez do conteúdo real (não é `robots.txt`, é WAF). Requisições
  repetidas escalam para HTTP 429 e depois bloqueio temporário do IP. Página
  de notícias identificada (`gov.br/anac/pt-br/noticias/ultimas-noticias-2`,
  possível RSS em `gov.br/anac/RSS`), mas não validada porque cai atrás do
  mesmo bloqueio. Diferente do bloqueio do Selenium (Brazil Journal/PPI): não
  há garantia de que um navegador automatizado passe por essa proteção, já
  que esse tipo de WAF costuma detectar browsers headless também — não
  implementar scraping até alguém validar acesso via navegador real dentro
  do cluster Databricks.
- **AGRESE** (`agrese.se.gov.br`, Saneamento — Sergipe): bloqueada, mas
  **diferente das outras acima** — não é WAF nem `robots.txt` (que permite
  tudo). URLs de *posts* de notícia devolvem HTTP 301 para uma página
  genérica (`https://www.se.gov.br/agencia`), enquanto páginas estáticas do
  mesmo site (`/institucional/`, `/portarias/`) respondem 200 normalmente —
  padrão que sugere bug de redirecionamento canônico no WordPress do
  governo de Sergipe, não decisão deliberada. Feed RSS funciona mas sem
  conteúdo novo há semanas; API REST (`/wp-json/wp/v2/posts`) trancada
  (401). Classificada como **provavelmente reversível** — retestar em
  algumas semanas antes de tentar de novo (ver `controle_bloqueios`,
  `blq_013`, e `registrar_bloqueio_agrese_saneamento.py`).
- **ARTESP** (`artesp.sp.gov.br`, Transporte — SP): bloqueada por dois
  problemas independentes, empilhados. (1) WAF ativo (Imperva/Incapsula)
  em `www.artesp.sp.gov.br` — a mesma URL, com o mesmo User-Agent de
  navegador, alterna entre devolver o HTML real da página e devolver uma
  página de challenge JS ("Pardon Our Interruption", cookies
  `visid_incap_*`/`incap_ses_*`/`nlbi_*`), sempre com HTTP 200 (nunca
  403/429) — mesma categoria de risco do bloqueio da ANAC (WAF F5/Shape),
  vendor diferente, comportamento mais intermitente (ANAC bloqueia quase
  sempre; aqui alterna). (2) Quando o WAF deixa passar: a página "Sala de
  Imprensa" (`/artesp/canais-de-comunicacao/sala-de-imprensa`) tem um
  portlet de listagem de notícias quebrado — exibe aviso nativo do CMS
  ("Configuração inválida localizada. Entre em contato com o
  administrador.") em vez da lista de itens; bug do lado da ARTESP,
  independente do WAF. (3) `/robots.txt` não é um robots.txt real —
  devolve o mesmo HTML da home institucional, não arquivo de diretivas.
  Não implementar scraping até validação com navegador real dentro do
  cluster Databricks — sem garantia de que resolveria (WAF pode detectar
  headless também), e o portlet quebrado precisaria ser corrigido do lado
  da ARTESP antes de qualquer captura fazer sentido de qualquer forma
  (ver `ingestores/TRANSPORTE/teste_artesp.ipynb`, `controle_bloqueios`,
  `blq_014`, e `registrar_bloqueio_artesp.py`).

## Notebooks e onde ficam no repo

| Notebook | Caminho | Fonte(s) |
|---|---|---|
| `ingest-news-rss-infra` | `ingestores/Notebooks unificados/` | RSS genérico (CreditoPrivado360, NeoFeed, Agência Eixos, Agência Infra) |
| `ingest-scraping` | `ingestores/Notebooks unificados/` | Scraping genérico (Acende Brasil, ANTT, AGESAN Notícias, PSR/Exame) |
| `ingest-PDF` | `ingestores/Notebooks unificados/` | PDFs genérico (EPE-SEGOV/MS, AGESAN Resoluções, CCEE Atas da Diretoria, ARPE Resoluções) |
| `ingest-diario-oficial-ms` | `ingestores/GERAL/` | Diário Oficial de MS |
| `ingest-news-infra-journal` | `ingestores/GERAL/` | Brazil Journal / INFRA Journal (Selenium) |
| `ingest-news-pipeline` | `ingestores/GERAL/` | Valor (pipelinevalor.globo.com) |
| `ingest-news-PPI-fixed` | `ingestores/GERAL/` | PPI |
| `ingest-news-PPI` (sem "-fixed") | `ingestores/GERAL/` | **status não confirmado** — existe no repo, ainda não sabemos se está em produção junto com o `-fixed` |
| `ingest-minutomega` | `ingestores/ENERGIA/Podcast MinutoMega/` | MinutoMega (RSS + transcrição via faster-whisper — lento, ~15-20min por episódio) |
| `ingest-news.ipynb` (raiz) | raiz do repo | **Em standby** — notebook didático original (Google News + queries de ativos nomeados). Está rodando em algum Job de produção, mas ainda não confirmamos se continua necessário. Não mexer até confirmar. |
| `scripts/utils_controle.py` | `scripts/` | Função compartilhada `atualizar_status_fonte` |
| `scripts/migrar_controle_excel_para_delta.py` | `scripts/` | Migração única (já executada) |
| `scripts/corrigir_source_id_duplicado.py` | `scripts/` | Correção única (já executada) |
| `scripts/rodar_testes_dispatchers.py` | `scripts/` | Notebook auxiliar de teste em lote (dispara vários dispatchers + consulta a tabela no final) |

**Pendente**: notebooks de Estadão, O Globo e Moody's ainda não foram
localizados/enviados — quando existirem, aplicar o mesmo padrão de
`atualizar_status_fonte`.

## Estilo de código já estabelecido no projeto

- Comentários e nomes de variável em português.
- Cada dispatcher genérico usa um dicionário `CONFIGS_FONTES` e um widget
  Databricks `fonte` (default `"todas"`) para rodar uma fonte específica ou
  todas de uma vez.
- Deduplicação via manifesto (`.json` com lista de URLs já processadas) em
  `/Volumes/desafio_kinea/research/research_volume/infraestrutura/manifests/`.
- `salvar_artefatos()` sempre grava um par `.txt` (texto limpo) +
  `.json` (metadados: `source_id`, `title`, `description`, `url`, `date`,
  `published_at`) — esse é o contrato fixo consumido pelo pipeline de
  sumarização/briefing (`gera_config.ipynb`).

## Como adicionar uma fonte nova

Fluxo em 3 fases — sempre nessa ordem, sem pular etapa:

**Fase 1 — Teste isolado.** Cria um notebook de teste em
`ingestores/[SETOR]/teste_[nome_fonte].ipynb`, sem depender de nenhum
dispatcher existente. Só valida se dá pra extrair os itens da fonte (RSS,
scraping ou PDF, o que fizer sentido) e imprime uma amostra pra conferência
manual. Esse notebook é descartável — não precisa seguir o padrão de
produção (sem `atualizar_status_fonte`, sem estar registrado em nenhuma
tabela).

**Fase 2 — Avaliação: encaixa num dispatcher genérico ou precisa de notebook
próprio?**
- Encaixa num dos 3 genéricos (`ingest-news-rss-infra`, `ingest-scraping`,
  `ingest-PDF`) se: é "baixar uma URL e extrair itens de forma padrão" —
  sem precisar de navegador, transcrição de áudio, ou parsing muito
  particular daquele site específico.
- Precisa de notebook próprio (como `ingest-diario-oficial-ms` ou
  `ingest-minutomega`) se: exige Selenium/Playwright, processamento de
  áudio, ou uma lógica de descoberta/parsing que não se resume a
  "baixar e extrair itens".

**Fase 3 — Integração**, dependendo do resultado da Fase 2:
- *Dispatcher genérico*: adiciona a entrada no `CONFIGS_FONTES`
  correspondente, com um `source_id` novo e **único** (confirma antes que
  não existe ainda — ver a query de duplicidade na seção acima). Adiciona
  a linha em `controle_fontes` (status inicial `"Não iniciada"`).
- *Notebook próprio*: cria o notebook em `ingestores/[SETOR]/`, seguindo o
  padrão dos notebooks de fonte única — todos os desfechos (sucesso, cada
  erro esperado) cobertos com `atualizar_status_fonte`, `%run` do
  `utils_controle` posicionado logo antes da célula de execução (não logo
  após o `restartPython()`, para evitar problema de timing). Adiciona
  linhas correspondentes em `controle_fontes`, `controle_notebooks` e
  `controle_tasks`.

Só depois da Fase 3 a fonte deve ser adicionada a algum Job de produção.

## Próximas etapas do plano (ordem sugerida)

1. ✅ Migração Excel → Delta
2. ✅ Dispatchers atualizando `controle_fontes` em tempo real
3. 🔄 Terminar de validar todos os dispatchers (Brazil Journal em teste;
   faltam Estadão/O Globo/Moody's; decidir sobre `ingest-news.ipynb`)
4. ✅ Este arquivo (`CLAUDE.md`)
5. ✅ Relatório HTML de progresso — em abas (Mensagens / Fontes / Notebooks
   & Tasks / Bloqueios / Fases & Entregáveis / Últimas atualizações), com
   fichas compactas por fonte e anotações manuais via `MENSAGENS.md`
6. ⬜ Continuar refinando o relatório conforme feedback de uso
7. ⬜ Processo de adicionar fontes novas (fluxo de 3 fases acima) — em uso,
   mas ainda não testado de ponta a ponta com uma fonte real

