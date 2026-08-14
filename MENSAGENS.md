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

**AGRESE — Saneamento (Sergipe)**, investigada em 14/08/2026, ainda sem
registro formal em `controle_fontes`/`controle_bloqueios`: o feed RSS
(`agrese.se.gov.br/feed/`) funciona e lista os itens normalmente, mas as
URLs de cada notícia devolvem HTTP 301 para uma página genérica sem
conteúdo — tanto o link antigo do WordPress (`agrese.se.gov.br/[slug]/`)
quanto o novo caminho do portal central do governo de Sergipe
(`www.se.gov.br/agencia/noticias/governo/[slug]`, achado via busca) estão
quebrados agora. O Google já indexou conteúdo real nesse portal no
passado, o que sugere instabilidade atual do lado do governo de Sergipe,
não bloqueio deliberado nem característica permanente. Vale retestar em
algumas semanas antes de decidir se encaixa no dispatcher genérico de RSS
(`ingest-news-rss-infra`, mesmo padrão da ARISB-MG) ou se precisa virar
bloqueio formal.

## Fases

(anotações sobre o andamento das fases/entregáveis do desafio)

## Atualizações

(anotações sobre o histórico de execuções)
