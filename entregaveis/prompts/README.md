# Prompts do pipeline de enriquecimento (NLP)

## Fonte única de verdade: `scripts/gera_config.ipynb`

Os prompts de verdade — os que de fato rodam em produção — vivem em
**[`scripts/gera_config.ipynb`](../../scripts/gera_config.ipynb)**, já
versionado no Git. Este arquivo (`docs/prompts/README.md`) **não duplica
o conteúdo deles** — só explica onde cada peça mora e como elas se
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
| `system_formatter_prompt` | `SYSTEM_FORMATTER_PROMPT` | Converte o Markdown final em HTML pronto para e-mail — seções empilhadas por setor (não abas, e-mail não roda JS), sumário executivo consolidado no topo, agrupamento de fontes por domínio |
| `css_for_doc` | `CSS_FOR_DOC` | CSS inline embutido no HTML do e-mail |
| `logo_data_uri` | `""` (vazio) | Reservado para logo embutido, não usado atualmente |

## Perfil diário vs. `config_infra_weekly.json`

O perfil semanal (célula 5) **reaproveita sem alteração** o Selector, o
gerador de tópicos e a priorização (`BUILD_SELECTOR_PROMPT`,
`SYSTEM_TOPICS_GENERATOR`, `TOPIC_PRIORITIZATION_PROMPT`) — a lógica de
"o que é relevante" não muda por ser semanal, só a janela de tempo (7
dias, agregados por `Processa_Weekly_Infra.ipynb`) e o texto final. Os
três componentes que **são próprios do semanal**:

- `SYSTEM_SUMMARIZER_PROMPT_INFRA_WEEKLY` — esqueleto mais compacto por
  item (Resumo em 3-5 linhas, sem "O que mudou"/"Por que importa"
  separados) e com um campo `**Data:**` que o diário não tem.
- `SYSTEM_FORMATTER_PROMPT_INFRA_WEEKLY` — além de formatar os itens da
  semana, gera uma seção adicional só do semanal, **Conclusão da
  Semana** (principais tendências, setores de maior risco, ranking das 5
  fontes que mais contribuíram, o que monitorar na próxima semana) — a
  única parte do pipeline onde o LLM tem instrução explícita de
  *sintetizar* em vez de só reestruturar o que já foi dado.
- `CSS_FOR_WEEKLY_DOC` — estende `CSS_FOR_DOC` com estilos próprios dos
  blocos semanais (`.weekly-item`, `.weekly-conclusion`,
  `.fontes-cobertas-section`, esta última montada por código Python, não
  pelo LLM).

Isso é proposital (comentário no próprio notebook, célula 4): mantém o
mecanismo diário, já validado, isolado de qualquer risco de interferência
do desenvolvimento do semanal — `Processa_Daily_Infra` não sabe que
`config_infra_weekly.json` existe.

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
