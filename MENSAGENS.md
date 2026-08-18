# Mensagens e anotações do projeto

Edite este arquivo à vontade para deixar recados para o time ou anotações em
qualquer seção do relatório. Não precisa de Databricks nem de SQL — é só um
arquivo de texto no repo. O relatório lê o conteúdo automaticamente na
próxima vez que for gerado.

Cada seção abaixo (`##`) corresponde a uma aba do relatório. Escreva o que
quiser embaixo do título — o relatório mostra esse texto como está.

## Geral

(recado geral do time, aparece na primeira aba do relatório)

## Fontes

(anotações sobre a cobertura de fontes)

## Notebooks e Tasks

(anotações sobre os dispatchers e o agendamento)

## Bloqueios

**AGRESE — Saneamento (Sergipe)**, formalizada como bloqueio em 18/08/2026
(`controle_fontes`/`controle_bloqueios`, `blq_013`), após reteste da
instabilidade anotada em 14/08/2026. Diagnóstico mais completo desta
rodada: `robots.txt` permite tudo; homepage e páginas estáticas
(`/institucional/`, `/portarias/`) respondem HTTP 200 normalmente; feed
RSS (`agrese.se.gov.br/feed/`) responde 200 e lista itens, mas o mais
recente é de 30/06/2026 — quase 7 semanas sem conteúdo novo. URLs de
**posts** de notícia (o que o feed lista) devolvem HTTP 301 para
`https://www.se.gov.br/agencia` (página genérica) — testado com/sem
`www.`, sem barra final, e via permalink "feio" (`?p=ID`), todos
convergem pro mesmo redirecionamento quebrado. API REST do WordPress
(`/wp-json/wp/v2/posts`) devolve 401, trancada. O padrão (só posts
quebrados, páginas estáticas OK) sugere bug de redirecionamento canônico
no WordPress do governo de Sergipe, não bloqueio deliberado — sem WAF,
sem `robots.txt` restritivo, sem CAPTCHA. Classificado como
**provavelmente reversível**; recomendado retestar em algumas semanas —
se voltar a funcionar, encaixa direto no dispatcher genérico de RSS
(`ingest-news-rss-infra`, mesmo padrão da ARISB-MG).

**ARTESP — Transporte (SP)**, formalizada como bloqueio em 18/08/2026
(`controle_fontes`/`controle_bloqueios`, `blq_014`), depois do teste
isolado de Fase 1 (`ingestores/TRANSPORTE/teste_artesp.ipynb`). Dois
problemas independentes, empilhados: (1) WAF ativo (Imperva/Incapsula) em
`www.artesp.sp.gov.br` — a mesma URL, com o mesmo User-Agent de navegador,
alterna entre devolver o HTML real da página e devolver uma página de
challenge JS ("Pardon Our Interruption", cookies
`visid_incap_*`/`incap_ses_*`/`nlbi_*`), sempre com HTTP 200 (nunca
403/429) — mesma categoria de risco do bloqueio da ANAC (WAF F5/Shape),
vendor diferente, comportamento mais intermitente; (2) quando o WAF deixa
passar, a página "Sala de Imprensa" tem um portlet de listagem de
notícias quebrado — exibe aviso nativo do CMS ("Configuração inválida
localizada. Entre em contato com o administrador.") em vez da lista de
itens, bug do lado da ARTESP, independente do WAF; (3) `/robots.txt` não
é um robots.txt real — devolve o mesmo HTML da home institucional.
Classificado como incerto se é reversível — depende de validação com
navegador real dentro do cluster Databricks (sem garantia, WAF pode
detectar headless) e de a própria ARTESP corrigir o portlet quebrado.

## Fases

(anotações sobre o andamento das fases/entregáveis do desafio)

## Atualizações

(anotações sobre o histórico de execuções)
