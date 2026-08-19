# Databricks notebook source
# =============================================================================
# chamar_llm.py
#
# Função compartilhada de chamada de LLM, usada por extrair_riscos_credito.py
# e montar_amostra_json_formal.py. Baseada no padrão real já usado no
# workspace (templates/Exemplo - GPT - Function Calling.ipynb) -- mesma
# conta Azure OpenAI, mesmo secret scope.
#
# DIFERENÇA proposital em relação ao exemplo: o exemplo usa Function
# Calling (schema por chamada). Aqui uso JSON via instrução de texto
# (json.loads na resposta) -- porque extrair_riscos_credito.py já foi
# construído com uma assinatura simples, chamar_llm(system, user), sem
# lugar pra passar um schema por chamada. Function Calling seria mais
# robusto (o modelo é forçado à estrutura), mas exigiria reescrever o
# módulo já pronto -- essa troca não parecia valer a pena só por isso.
#
# CONFERIR ANTES DE RODAR EM PRODUÇÃO:
#   1. O nome do "engine" (`gpt35turbo16k` no exemplo) pode não ser o
#      disponível/aprovado para este caso de uso -- confirmar com quem
#      administra a conta Azure OpenAI (akvanalyticsdevkna).
#   2. O secret scope 'akvanalyticsdevkna' precisa estar acessível pro seu
#      usuário/cluster -- se der erro de permissão aqui, é isso.
# =============================================================================

# COMMAND ----------

# MAGIC %pip install openai==0.28.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re
import json
import openai

openai.api_type = "azure"
openai.api_base = "https://openaikinea.openai.azure.com/"
openai.api_version = "2023-12-01-preview"
openai.api_key = dbutils.secrets.get('akvanalyticsdevkna', 'openai')

LLM_ENGINE = "gpt35turbo16k"  # confirmar se é o engine certo pra este caso de uso

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
    completion = openai.ChatCompletion.create(
        engine=LLM_ENGINE,
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
        stop=None,
    )

    texto_resposta = completion.to_dict()["choices"][0]["message"]["content"]
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
