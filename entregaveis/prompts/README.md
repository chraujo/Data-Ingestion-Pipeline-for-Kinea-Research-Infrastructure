# Prompts do pipeline de enriquecimento (NLP)

## Fonte única de verdade: `scripts/gera_config.ipynb`

Os prompts de verdade — os que de fato rodam em produção — vivem em
**[`scripts/gera_config.ipynb`](../../scripts/gera_config.ipynb)**, já
versionado no Git. Este arquivo (`entregaveis/prompts/README.md`) **não
duplica o conteúdo deles** — só explica onde cada peça mora e como elas se
encaixam, para quem estiver navegando o repositório pela primeira vez.
Qualquer mudança de comportamento (redação de um prompt, critério de
seleção, formato de saída) deve ser feita direto em
`scripts/gera_config.ipynb` — nunca copiada/colada para outro lugar,
porque isso criaria risco real de dessincronia (alguém edita a cópia,
esquece de propagar para o original, e o comportamento em produção
diverge silenciosamente do que está documentado).

> Até 2026-08-14 existia uma segunda cópia, `gera_config.ipynb` na raiz do
> repositório, desatualizada (ainda gerava HTML com abas clicáveis em
> JavaScript, que não funcionam em clientes de e-mail) e que não era a
> usada em produção — já foi removida (commit "apaga gera config antigo").
> Fica registrado aqui só como contexto histórico: se você encontrar
> referências a uma cópia na raiz em material mais antigo, ela não existe
> mais — a fonte única sempre foi, e continua sendo,
> `scripts/gera_config.ipynb`.

## O que `gera_config.ipynb` faz

Roda como a primeira task (`gera-config`) do Job semanal
(`Resumo Semanal INFRA`, sexta-feira). Define todos os prompts como
strings Python, injeta a lista de empresas de carteira Kinea (lida de
`scripts/canonical_entidades.json` — mesmo arquivo usado por
`scripts/extrair_riscos_credito.py`, também fonte única, nunca duplicar),
monta dois dicionários de configuração e grava cada um como JSON no
Volume:

- `config_N.json` (perfil **diário**) em
  `/Volumes/desafio_kinea/research/research_volume/infraestrutura/prompts/`
  — `N` é sempre o maior número já existente na pasta + o próprio
  notebook decide o próximo (célula 3). `Processa_Daily_Infra` sempre lê
  o `config_N.json` de maior número, não um nome fixo.
- `config_infra_weekly.json` (perfil **semanal**) — nome de arquivo fixo
  (célula 5), lido exclusivamente por `Processa_Weekly_Infra.ipynb`.

## Onde cada prompt vive (perfil diário)

Todos definidos como constantes Python na célula 0 de
`scripts/gera_config.ipynb`, e reunidos no dicionário `config` na célula 2:

| Chave no `config_N.json` | Constante Python | Papel |
|---|---|---|
| `build_selector_prompt` | `BUILD_SELECTOR_PROMPT` | Prompt de sistema do estágio de triagem de relevância — critérios de inclusão/exclusão, prioridade de carteira Kinea, framework de avaliação (regulatório, leilões, legislação, dados operacionais, eventos corporativos, ESG, macro) |
| `user_selector_prompt` | `USER_SELECTOR_PROMPT` | Template do prompt de usuário para o Selector — injeta a lista de itens pré-capturados |
| `system_selector_intro` | `SYSTEM_SELECTOR_INTRO` | Variante de formato de saída do Selector (lista de índices selecionados) |
| `system_selector_intro_bool` | `SYSTEM_SELECTOR_INTRO_BOOL` | Variante de formato de saída do Selector (booleano relevante/não relevante por item) |
| `system_selector_2stg_intro` | `SYSTEM_SELECTOR_2STG_INTRO` | Variante para triagem em cima de clusters já agrupados (2 estágios), não itens individuais |
| `system_topics_generator` | `SYSTEM_TOPICS_GENERATOR` | Agrupa itens selecionados em clusters temáticos, com nome e descrição em português |
| `topic_prioritization_prompt` | `TOPIC_PRIORITIZATION_PROMPT` | Ranqueia os clusters e seleciona os top N mais relevantes para o dia |
| `system_summarizer_prompt` | `SYSTEM_SUMMARIZER_PROMPT` | Escreve cada seção do briefing (esqueleto fixo: Resumo Executivo, O que mudou, Por que importa, Impacto Esperado, Próximos Gatilhos, Potencial Impacto na Carteira, Fontes) |
| `system_minor_topic_summarizer` | `SYSTEM_MINOR_TOPIC_SUMMARIZER` | Bullet compacto para temas secundários (não promovidos a seção completa) |
| `system_check_sell_side_prompt` | `""` (vazio) | Reservado, não usado atualmente |
| `system_formatter_prompt` | `SYSTEM_FORMATTER_PROMPT` | Converte o Markdown final em HTML pronto para e-mail — 3 partes fixas: Sumário Executivo (índice compacto, único lugar com o selo de Impacto Esperado), Notícia do Dia (tema #1, único, tratamento completo) e Notícias Secundárias (os demais temas, condensados, grade de 2 colunas). Não há mais seções por setor nem abas clicáveis (e-mail não roda JS); a posição no Markdown (tema #1 vs. resto) decide o que vira Notícia do Dia, não o rating de impacto |
| `css_for_doc` | `CSS_FOR_DOC` | CSS inline embutido no HTML do e-mail |
| `logo_data_uri` | `""` (vazio) | Reservado para logo embutido, não usado atualmente |

## Resumo Semanal: perfil de prompt próprio, não apêndice do diário

O Resumo Semanal (sexta-feira) tem **perfil de prompt separado** do
diário — grava/lê um arquivo de nome fixo próprio,
`config_infra_weekly.json` (célula 5 de `gera_config.ipynb`), diferente
do `config_N.json` numerado do diário. `Processa_Daily_Infra` não sabe
que `config_infra_weekly.json` existe, e vice-versa — isolamento
proposital (comentário no próprio notebook, célula 4): mantém o mecanismo
diário, já validado, livre de qualquer risco de interferência do
desenvolvimento do semanal.

O perfil semanal **reaproveita sem alteração** o Selector, o gerador de
tópicos e a priorização (`BUILD_SELECTOR_PROMPT`, `SYSTEM_TOPICS_GENERATOR`,
`TOPIC_PRIORITIZATION_PROMPT`) — a lógica de "o que é relevante" não muda
por ser semanal, só a janela de tempo (últimos 7 dias, agregados por
`Processa_Weekly_Infra.ipynb`, não só o dia corrente) e o texto final. Os
três componentes que **são próprios do semanal**:

- `SYSTEM_SUMMARIZER_PROMPT_INFRA_WEEKLY` — esqueleto bem mais compacto
  que o diário: um único bullet por tema (fato + por que importa,
  relevância de carteira dobrada na mesma frase quando aplicável), sem as
  seções separadas "O que mudou"/"Por que importa"/"Próximos Gatilhos"
  do diário. Esse bullet por tema é só o ingrediente bruto — quem
  realmente monta o documento final é o Formatter, a seguir.
- `SYSTEM_FORMATTER_PROMPT_INFRA_WEEKLY` — **redesenho deliberado**: em
  vez de um card por tema, tece os bullets da semana numa **narrativa
  única em parágrafos corridos**, tipo memorando de analista, organizada
  sob 3 subtítulos fixos em formato de pergunta, nesta ordem exata:
  1. "Quais foram os principais vetores da semana?"
  2. "Quais setores concentraram o maior risco?"
  3. "O que merece atenção na próxima semana?"

  Cada parágrafo tem limite de ~4 linhas (~60-80 palavras); nem todo tema
  da semana precisa aparecer na prosa (preferível deixar de fora um tema
  menor a estourar o limite). Fecha com uma lista compacta (não prosa) das
  5 fontes que mais contribuíram na semana. Esta é a única parte do
  pipeline onde o LLM tem instrução explícita de *sintetizar* em vez de só
  reestruturar o que já foi dado.
- `CSS_FOR_WEEKLY_DOC` — estende `CSS_FOR_DOC` com estilos próprios dos
  blocos semanais (`.weekly-narrative`, `.narrative-question`,
  `.source-ranking`, `.fontes-cobertas-section` — esta última montada por
  código Python, não pelo LLM).

## Pipeline de NLP formal (`scripts/montar_amostra_json_formal.py`)

Pipeline separado do briefing por e-mail (não usa `gera_config.ipynb`),
que existe pra atender ao schema oficial da Seção 5 do briefing do
desafio. Vale a pena documentar aqui porque é onde o princípio de
**separar decisão de código (determinístico, auditável) de julgamento do
LLM (sujeito a erro/alucinação)** aparece de forma mais explícita no
projeto — o LLM nunca decide sozinho um resultado que precisa ser
reproduzível ou auditável; ele gera candidatos/sinais brutos, e código
determinístico decide o resultado final. Três exemplos reais:

**(a) Extração de entidades — `scripts/extrair_riscos_credito.py`.** O
LLM só extrai nomes de empresas/grupos citados no texto — nunca tenta
adivinhar o `canonical_id` sozinho. Um fuzzy match determinístico
(`rapidfuzz`, limiar fixo `LIMIAR_MATCH = 0.85`) resolve esse nome contra
`scripts/canonical_entidades.json` e decide o `canonical_id` (ou `null`,
se não bater com nada). Motivo (comentário no próprio arquivo): deixar o
LLM "decidir" o ID sozinho seria uma caixa-preta sujeita a alucinar um
`canonical_id` que não existe; um fuzzy match com threshold fixo é
conferível e reproduzível.

**(b) Vocabulário de tags fechado — `classificar_tags()` em
`montar_amostra_json_formal.py`.** O prompt já instrui o LLM a usar só o
vocabulário fechado (lista abaixo), mas o código **não confia na
instrução sozinha** — filtra a resposta e descarta qualquer tag fora da
lista antes dela chegar no resultado final, mesmo que o LLM erre e invente
uma tag nova (fica um aviso no log, `tag_sugerida_fora_do_vocabulario`
fica disponível como sugestão pra revisão humana, mas nunca entra como
tag oficial).

**(c) `relevance_score` — mesmo arquivo, `calcular_relevance_score()`.**
Calculado inteiramente em código, a partir de 4 sinais estruturados —
**nunca decidido pelo modelo**:

| Fator | Peso | Como é calculado |
|---|---|---|
| Prioridade da fonte | 0.30 | `Alta`→1.0, `Média`→0.6, `Baixa`→0.3 (de `controle_fontes.importancia_original`) |
| Presença de portfólio | 0.35 | 0 empresas→0.0, 1→0.5, 2-3→0.75, 4+→1.0 (contagem de `credit_risks_mentioned` com `in_kinea_universe=true`) |
| Valor monetário relevante | 0.15 | 1.0 se o texto menciona valor ≥ R$ 100 milhões (regex sobre `R$ ... milhão/bilhão`), senão 0.0 |
| Tags críticas | 0.20 | 1.0 se pelo menos uma tag da notícia está em `TAGS_CRITICAS`, senão 0.0 |

`score = 0.30×prioridade + 0.35×portfólio + 0.15×valor + 0.20×tags`,
arredondado em 3 casas. O `relevance_score_rationale` (string, salvo junto
no JSON de cada notícia) documenta os 4 termos e o resultado por extenso
— auditável por notícia, não só a fórmula em abstrato.

### Vocabulário fechado de tags

Definido em `VOCABULARIO_TAGS`, em `montar_amostra_json_formal.py`:

```
fato_relevante, comunicado_cvm, resultado_trimestral,
emissao_debenture, emissao_cri, emissao_cra, captacao,
m_a, ipo, follow_on,
rating_change,
default, renegociacao, recuperacao_judicial,
litigation, regulatorio, governanca,
guidance, projeto_novo, expansao, desinvestimento,
esg, safra, lancamento
```

`TAGS_CRITICAS` (subconjunto usado no peso de tags do `relevance_score`,
tabela acima): `fato_relevante`, `rating_change`, `m_a`, `default`,
`litigation`.

### Clustering semântico (Seção 7.1 do briefing)

`atribuir_clusters()` usa embeddings (não mais comparação de string por
título) — texto de entrada é título + summary, gerados via
`gerar_embedding_local()` (`scripts/chamar_llm.py`, modelo local
`sentence-transformers`, 384 dimensões — a conta Azure OpenAI não tem
deployment de embedding disponível). Similaridade de cosseno, limiar
`LIMIAR_CLUSTER_SIMILARIDADE_EMBEDDING = 0.75` (calibrado empiricamente,
ver comentário junto da constante no código), janela deslizante de ±72h
(compara candidatos entre dias adjacentes, não fica isolado por dia). O
método anterior (comparação de título por `SequenceMatcher`, isolado por
dia) foi mantido no arquivo como
`atribuir_clusters_por_titulo_LEGADO()` — não usado em produção, serve de
fallback/comparação.

## Lista de empresas de carteira (portfolio Kinea)

Injetada em tempo de geração do config (célula 1 de `gera_config.ipynb`),
a partir de `scripts/canonical_entidades.json` — usada para:

1. Sinalizar prioridade de seleção/priorização quando um item menciona uma
   empresa do portfólio (`BUILD_SELECTOR_PROMPT`,
   `TOPIC_PRIORITIZATION_PROMPT`, versão compacta só com nomes).
2. Marcar inline (`{portfolio:Nome (TICKER)}`) e alimentar a seção
   "Potencial Impacto na Carteira" no corpo do briefing
   (`SYSTEM_SUMMARIZER_PROMPT`/`_INFRA_WEEKLY`, versão completa com
   tickers).

Editar a lista **sempre** em `canonical_entidades.json` — é o mesmo
arquivo usado por `scripts/extrair_riscos_credito.py`, então uma edição
ali já propaga para os dois usos sem precisar duplicar nada.
