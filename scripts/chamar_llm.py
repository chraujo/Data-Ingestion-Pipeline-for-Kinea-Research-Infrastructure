# Databricks notebook source
# =============================================================================
# chamar_llm.py
#
# Função compartilhada de chamada de LLM, usada por extrair_riscos_credito.py
# e montar_amostra_json_formal.py. Mesma conta Azure OpenAI e mesmo secret
# scope já usados no workspace (visto em
# templates/Exemplo - GPT - Function Calling.ipynb).
#
# REVISADO: a primeira versão deste arquivo tentava instalar
# `openai==0.28.1` (sintaxe antiga, igual ao notebook de exemplo) -- só
# que o cluster já tem openai 2.14.0 instalado, do qual OUTRAS coisas no
# mesmo cluster dependem (langchain-openai, litellm). Forçar a versão
# antiga conflitava com esse ambiente e deixava tudo num estado quebrado.
# Esta versão usa a sintaxe ATUAL da biblioteca (client.chat.completions),
# sem instalar nem trocar versão nenhuma -- só usa o que já está lá.
#
# DIFERENÇA proposital em relação ao exemplo: o exemplo usa Function
# Calling (schema por chamada). Aqui uso JSON via instrução de texto
# (json.loads na resposta) -- porque extrair_riscos_credito.py já foi
# construído com uma assinatura simples, chamar_llm(system, user), sem
# lugar pra passar um schema por chamada.
#
# CONFERIR ANTES DE RODAR EM PRODUÇÃO:
#   1. O nome do "model"/deployment (`gpt35turbo16k` no exemplo antigo)
#      pode não ser o disponível/aprovado para este caso de uso --
#      confirmar com quem administra a conta Azure OpenAI.
#   2. O secret scope 'akvanalyticsdevkna' precisa estar acessível pro seu
#      usuário/cluster -- se der erro de permissão aqui, é isso.
# =============================================================================

# COMMAND ----------

import re
import json
from openai import AzureOpenAI

client = AzureOpenAI(
    api_key=dbutils.secrets.get('akvanalyticsdevkna', 'openai'),
    api_version="2023-12-01-preview",
    azure_endpoint="https://openaikinea.openai.azure.com/",
)

LLM_ENGINE = "gpt35turbo16k"  # confirmar se é o deployment certo pra este caso de uso

# COMMAND ----------

def _limpar_resposta_json(texto: str) -> str:
    """Remove markdown fences (```json ... ```) que o modelo às vezes
    adiciona mesmo quando instruído a não fazer isso -- defensivo, não
    assume que o modelo obedece 100%."""
    texto = texto.strip()
    texto = re.sub(r"^```(?:json)?\s*\n?", "", texto)
    texto = re.sub(r"\n?```\s*$", "", texto)
    return texto.strip()


def chamar_llm(system: str, user: str) -> dict:
    """Chamada de LLM que devolve um dict Python -- usada por
    extrair_riscos_credito.py e montar_amostra_json_formal.py.

    O prompt (`system`) já pede explicitamente pra resposta vir em JSON --
    essa função só faz a chamada e faz o parse, sem adicionar Function
    Calling por cima (ver nota no topo do arquivo)."""
    completion = client.chat.completions.create(
        model=LLM_ENGINE,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,   # baixa de propósito -- essas chamadas são de
                            # classificação/extração, não de texto criativo
        max_tokens=1500,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
    )

    texto_resposta = completion.choices[0].message.content
    texto_limpo = _limpar_resposta_json(texto_resposta)

    try:
        return json.loads(texto_limpo)
    except json.JSONDecodeError as e:
        # Não engole o erro silenciosamente -- melhor falhar visivelmente
        # (e o chamador decide se pula essa notícia) do que devolver algo
        # incorreto sem avisar.
        raise ValueError(
            f"Resposta do LLM não é JSON válido: {e}\nResposta bruta: {texto_resposta[:500]}"
        )

