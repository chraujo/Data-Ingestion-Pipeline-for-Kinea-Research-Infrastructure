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
  - ID: `2082316997975725`
  - Caminho no Workspace: `/Workspace/Shared/Research_Infra/Data-Ingestion-Pipeline-for-Kinea-Research-Infrastructure`

### Fluxo de sincronização (não é automático — cada lado precisa de ação manual)

```
edita local (VS Code) → git push → databricks repos update 2082316997975725 --branch main --profile kinea-desafio
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

## Notebooks e onde ficam no repo

| Notebook | Caminho | Fonte(s) |
|---|---|---|
| `ingest-news-rss-infra` | `ingestores/Notebooks unificados/` | RSS genérico (CreditoPrivado360, NeoFeed, Agência Eixos, Agência Infra) |
| `ingest-scraping` | `ingestores/Notebooks unificados/` | Scraping genérico (Acende Brasil, ANTT, AGESAN Notícias, PSR/Exame) |
| `ingest-PDF` | `ingestores/Notebooks unificados/` | PDFs genérico (EPE-SEGOV/MS, AGESAN Resoluções) |
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

## Próximas etapas do plano (ordem sugerida)

1. ✅ Migração Excel → Delta
2. ✅ Dispatchers atualizando `controle_fontes` em tempo real
3. 🔄 Terminar de validar todos os dispatchers (Brazil Journal em teste;
   faltam Estadão/O Globo/Moody's; decidir sobre `ingest-news.ipynb`)
4. 🔄 Este arquivo (`CLAUDE.md`)
5. ⬜ Script gerador de relatório HTML de progresso (estilo visual do
   `exemplo_resultado_infra.html`), lendo de `controle_fontes` — seções
   ativas (placar geral, cobertura por área, pendências, últimas
   atualizações) + seções em standby (entregáveis, critérios de aceitação,
   timeline, estimativa de custo — do briefing formal do desafio)
6. ⬜ Testar o relatório e validar pra circular em standup
