# kinea-infra-ingestao

Automated ingestion pipeline for news, regulatory data, and podcast sources in the infrastructure sector, feeding Kinea's daily research briefing.

## Overview

The project captures content from multiple sources (news sites, regulatory agencies, institutional PDFs, podcasts), normalizes everything into a canonical schema, and makes the data available for the LLM enrichment stage (selection, summarization, final briefing formatting).

Each source has a different capture architecture, depending on how the content is published — RSS, direct scraping, PDF, transcribed audio. Instead of one notebook per source, sources that share the same architecture are grouped into **parameterized dispatchers**: a single notebook that receives the source name as a parameter and processes it accordingly.
