from __future__ import annotations
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from pydantic import ValidationError
from .modelos import AnaliseIA, schema_json

PASTA_PROMPTS = Path(__file__).resolve().parents[1] / "prompts"
VERSAO_PROMPT_PADRAO = "analise_v1"
MODELO_PADRAO = "claude-sonnet-5-5"   # sobrescreva com ANTHROPIC_MODEL no .env
MAX_TENTATIVAS = 2                    # 1 resposta + 1 pedido de correcao

ChamarModelo = Callable[[str, list[dict]], str]


class ErroAnalise(Exception):
    """A resposta nao pode ser transformada em AnaliseIA."""


# --------------------------------------------------------------------- prompt
def carregar_prompt(versao: str = VERSAO_PROMPT_PADRAO) -> str:
    caminho = PASTA_PROMPTS / f"{versao}.md"
    texto = caminho.read_text(encoding="utf-8")
    texto = re.sub(r"<!--.*?-->\s*", "", texto, count=1, flags=re.S)   # tira o comentario de cabecalho
    # Schema sem indentacao: ~40% menos tokens, mesmo conteudo.
    texto = texto.replace("{{SCHEMA_JSON}}", json.dumps(schema_json(), ensure_ascii=False, separators=(",", ":")))
    if "{{" in texto:
        raise ValueError(f"Placeholder nao substituido em {caminho}")
    return texto.strip()


def montar_mensagem(resumo: dict) -> str:
    corpo = json.dumps(resumo, ensure_ascii=False, separators=(",", ":"))
    return (f"<resumo_ia>\n{corpo}\n</resumo_ia>\n\n"
            "Analise o resumo acima seguindo as regras e responda somente com o JSON.")


# ----------------------------------------------------------------- resposta
def extrair_json(texto: str) -> str:
    """Tira cercas ```json e qualquer texto fora do primeiro {...} ate o ultimo }."""
    inicio, fim = texto.find("{"), texto.rfind("}")
    if inicio == -1 or fim <= inicio:
        raise ErroAnalise("A resposta nao contem um objeto JSON.")
    return texto[inicio:fim + 1]


ASPAS_TIPOGRAFICAS = str.maketrans({"\u201c": '"', "\u201d": '"', "\u2018": "'", "\u2019": "'"})


def validar_resposta(texto: str) -> AnaliseIA:
    bruto = extrair_json(texto)
    try:
        return AnaliseIA.model_validate_json(bruto)
    except ValidationError as e:
        # Texto copiado de uma resposta renderizada pode trazer aspas curvas (“ ”) no
        # lugar de aspas retas. So tentamos trocar se o JSON original nao for valido.
        if "\u201c" in bruto or "\u201d" in bruto:
            try:
                return AnaliseIA.model_validate_json(bruto.translate(ASPAS_TIPOGRAFICAS))
            except ValidationError:
                pass
        raise ErroAnalise(_resumir_erro(e)) from e


def _resumir_erro(e: ValidationError) -> str:
    linhas = []
    for err in e.errors()[:15]:
        local = ".".join(str(x) for x in err["loc"]) or "(raiz)"
        linhas.append(f"- {local}: {err['msg']}")
    return "\n".join(linhas)


@dataclass
class ResultadoIA:
    analise: AnaliseIA | None
    tentativas: int
    erros: list[str] = field(default_factory=list)
    resposta_bruta: str = ""


def analisar(resumo: dict, chamar: ChamarModelo, versao: str = VERSAO_PROMPT_PADRAO,
             max_tentativas: int = MAX_TENTATIVAS) -> ResultadoIA:
    system = carregar_prompt(versao)
    mensagens = [{"role": "user", "content": montar_mensagem(resumo)}]
    erros: list[str] = []
    texto = ""
    for tentativa in range(1, max_tentativas + 1):
        texto = chamar(system, list(mensagens))
        try:
            return ResultadoIA(validar_resposta(texto), tentativa, erros)
        except ErroAnalise as e:
            erros.append(str(e))
            mensagens += [
                {"role": "assistant", "content": texto},
                {"role": "user", "content": mensagem_de_correcao(str(e))},
            ]
    return ResultadoIA(None, max_tentativas, erros, resposta_bruta=texto)


def mensagem_de_correcao(erro: str) -> str:
    return ("Sua resposta nao passou na validacao do schema:\n"
            f"{erro}\n\n"
            "Devolva o JSON completo corrigido, seguindo o schema, sem texto fora do JSON.")


# ---------------------------------------------------------------- provedores
def provedor_anthropic(modelo: str | None = None, max_tokens: int = 8000) -> ChamarModelo:
    """Chama a API da Anthropic. Requer `pip install anthropic` e ANTHROPIC_API_KEY."""
    from anthropic import Anthropic   # import aqui: so e necessario neste modo

    modelo = modelo or os.getenv("ANTHROPIC_MODEL", MODELO_PADRAO)
    cliente = Anthropic()             # le ANTHROPIC_API_KEY do ambiente

    def chamar(system: str, mensagens: list[dict]) -> str:
        r = cliente.messages.create(model=modelo, max_tokens=max_tokens,
                                    system=system, messages=mensagens)
        return "".join(b.text for b in r.content if getattr(b, "type", "") == "text")

    chamar.descricao = f"anthropic:{modelo}"
    return chamar


MODELO_GROQ_PADRAO = "openai/gpt-oss-120b"   # sobrescreva com GROQ_MODEL no .env


def provedor_groq(modelo: str | None = None, max_tokens: int = 3000) -> ChamarModelo:
    """Chama a API do Groq. Requer `pip install groq` e GROQ_API_KEY.

    * response_format=json_object: o Groq garante que a saida e um JSON valido
      (o formato do schema continua sendo conferido pelo Pydantic).
    * reasoning_effort="low" (modelos gpt-oss): gasta menos tokens pensando, o que
      importa no plano gratuito, que conta entrada + saida no limite por minuto.
    """
    from groq import Groq   # import aqui: so e necessario neste modo

    modelo = modelo or os.getenv("GROQ_MODEL", MODELO_GROQ_PADRAO)
    cliente = Groq()        # le GROQ_API_KEY do ambiente
    extras = {"reasoning_effort": "low"} if modelo.startswith("openai/gpt-oss") else {}

    def chamar(system: str, mensagens: list[dict]) -> str:
        r = cliente.chat.completions.create(
            model=modelo,
            messages=[{"role": "system", "content": system}, *mensagens],
            response_format={"type": "json_object"},
            temperature=0,
            max_completion_tokens=max_tokens,
            **extras,
        )
        return r.choices[0].message.content or ""

    chamar.descricao = f"groq:{modelo}"
    return chamar


def estimar_tokens(resumo: dict, versao: str = VERSAO_PROMPT_PADRAO) -> int:
    """Estimativa grosseira (JSON e texto em PT ~ 3,5 caracteres por token)."""
    return int((len(carregar_prompt(versao)) + len(montar_mensagem(resumo))) / 3.5)


def provedor_arquivo(caminho: Path) -> ChamarModelo:
    """Modo manual: a 'resposta do modelo' e o conteudo de um arquivo salvo por voce."""
    def chamar(system: str, mensagens: list[dict]) -> str:
        return Path(caminho).read_text(encoding="utf-8")

    chamar.descricao = f"manual:{Path(caminho).name}"
    return chamar


# --------------------------------------------------------------------- saida
def gravar_prompt_manual(resumo: dict, destino: Path, versao: str = VERSAO_PROMPT_PADRAO) -> Path:
    """Gera o texto para colar no Claude (modo manual)."""
    destino.write_text(
        "<!-- Cole TUDO abaixo numa conversa nova do Claude (fora de Projects com outras\n"
        "     instrucoes) e salve a resposta, so o JSON, num arquivo .json -->\n\n"
        f"{carregar_prompt(versao)}\n\n---\n\n{montar_mensagem(resumo)}\n\n"
        "(Neste chat, coloque o JSON dentro de um bloco de codigo ```json para que eu "
        "possa copia-lo pelo botao de copiar sem alterar as aspas.)\n",
        encoding="utf-8")
    return destino


def gravar_resultado(resultado: ResultadoIA, resumo: dict, destino: Path, provedor: str,
                     versao: str = VERSAO_PROMPT_PADRAO) -> dict:
    enviado = montar_mensagem(resumo).encode("utf-8")
    saida = {
        "meta_ia": {
            "prompt_versao": versao,
            "provedor": provedor,
            "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "resumo_enviado_sha256": hashlib.sha256(enviado).hexdigest(),
            "tentativas": resultado.tentativas,
            "valida": resultado.analise is not None,
            "erros_de_validacao": resultado.erros,
        },
        "analise": resultado.analise.model_dump(mode="json") if resultado.analise else None,
    }
    if resultado.analise is None:
        saida["resposta_bruta"] = resultado.resposta_bruta
    destino.write_text(json.dumps(saida, indent=2, ensure_ascii=False), encoding="utf-8")
    return saida