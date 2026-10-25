"""Script .py executado no EMR como contexto extra para a IA.

O script entra no resumo_ia.json na chave ``script``. Assim o hash do que foi
enviado, a conferencia de evidencias (ex.: ``script.linhas.123``) e o relatorio
continuam funcionando sem caminho paralelo.

Antes de sair daqui o codigo passa por:
  1. remocao de comentarios (``tokenize``: nao quebra strings com '#');
  2. o MESMO Sanitizador do log (path_N/host_N batem com os do resumo);
  3. mascaras proprias de codigo: e-mails e literais de credenciais.

O script nunca substitui as metricas: ele so ajuda a localizar no codigo o que
as metricas ja apontaram.
"""
from __future__ import annotations
import hashlib
import io
import re
import tokenize
from pathlib import Path, PurePosixPath, PureWindowsPath
from .padroes import CALL_SITE_PATTERN
from .sanitizador import Sanitizador

SCRIPT_VERSION = "1.0"
PASTA_SCRIPTS = Path("scripts")

MAX_LINHAS_COMPLETO = 600     # acima disso o script ja sai em modo "trechos"
MAX_CHARS_LINHA = 300
CONTEXTO_VINCULO = 3          # linhas antes/depois de uma linha citada pelo log
CONTEXTO_PALAVRA = 1          # linhas antes/depois de uma operacao Spark

# Operacoes que costumam ter custo no Spark. Usadas SO para escolher trechos
# quando o script nao cabe inteiro; nao sao evidencia de nada.
PALAVRAS_SPARK = re.compile(
    r"\.(?:join|crossJoin|groupBy|groupby|agg|distinct|dropDuplicates|orderBy|sort|"
    r"repartition|coalesce|cache|persist|unpersist|collect|toPandas|count|show|take|"
    r"write|save|saveAsTable|insertInto|merge|read|load|table|sql|union|unionByName|"
    r"withColumn|explode|udf|pandas_udf|broadcast|checkpoint|foreach|rdd)\b"
    r"|\bWindow\b|\bbroadcast\(|\bspark\.conf\.set\b|\bDeltaTable\b|\bfor\s+\w+\s+in\b"
)

EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
AWS_ACCESS_KEY = re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")
SEGREDO_ATRIBUIDO = re.compile(
    r"""(?P<chave>["']?\b[\w-]*(?:password|passwd|senha|secret|token|api[_-]?key|access[_-]?key|"""
    r"""private[_-]?key)[\w-]*\b["']?\s*[:=]\s*)(?P<q>["'])(?P<valor>[^"']+)(?P=q)""",
    re.IGNORECASE,
)

ORDEM_MODOS = ["completo", "trechos"]


# ----------------------------------------------------------------- arquivos
def listar_scripts(pasta: Path = PASTA_SCRIPTS) -> list[Path]:
    if not pasta.is_dir():
        return []
    return sorted((p for p in pasta.rglob("*.py") if p.is_file()), key=lambda p: str(p).lower())


def sugerir_script(scripts: list[Path], *nomes: str | None) -> Path | None:
    """Script cujo nome (sem extensao) aparece no nome do log ou da aplicacao."""
    alvos = [n.lower() for n in nomes if n]
    candidatos = [s for s in scripts if len(s.stem) >= 4
                  and any(s.stem.lower() in a or a in s.stem.lower() for a in alvos)]
    return max(candidatos, key=lambda s: len(s.stem)) if candidatos else None


# --------------------------------------------------------------- limpeza
def _sem_comentarios(texto: str) -> tuple[list[str], int]:
    """Remove comentarios preservando a numeracao das linhas."""
    linhas = texto.splitlines()
    cortes: dict[int, int] = {}
    try:
        for tok in tokenize.generate_tokens(io.StringIO(texto).readline):
            if tok.type == tokenize.COMMENT:
                cortes[tok.start[0]] = tok.start[1]
    except (tokenize.TokenError, IndentationError, SyntaxError):
        # Codigo que nao tokeniza: tira so as linhas que sao comentario inteiro.
        cortes = {i: 0 for i, l in enumerate(linhas, start=1) if l.lstrip().startswith("#")}
    for n, col in cortes.items():
        if n <= len(linhas):
            linhas[n - 1] = linhas[n - 1][:col].rstrip()
    return linhas, len(cortes)


def _mascarar_linha(linha: str, sanitizador: Sanitizador, contagem: dict) -> str:
    linha, n = SEGREDO_ATRIBUIDO.subn(lambda m: f"{m['chave']}{m['q']}<redigido>{m['q']}", linha)
    contagem["segredos"] += n
    linha, n = AWS_ACCESS_KEY.subn("<redigido>", linha)
    contagem["segredos"] += n
    linha, n = EMAIL.subn("<email>", linha)
    contagem["emails"] += n
    return sanitizador.mask_text(linha)


# --------------------------------------------------------------- vinculos
def _basename(arquivo: str) -> str:
    return PureWindowsPath(PurePosixPath(arquivo).name).name.lower()


def _nome_real(arquivo: str, sanitizador: Sanitizador) -> str:
    """'path_3' -> nome do arquivo original (o mapa do sanitizador e reversivel aqui)."""
    for bruto, pseudo in sanitizador.path_map.items():
        if pseudo == arquivo:
            return _basename(bruto.rsplit(":", 1)[0] if re.search(r":\d+$", bruto) else bruto)
    return _basename(arquivo)


def _linha_do_call_site(acao: str | None, call_site: dict | None, nome_script: str,
                        sanitizador: Sanitizador) -> int | None:
    cs = call_site
    if not cs and acao:
        m = CALL_SITE_PATTERN.match(acao)
        cs = {"arquivo": m["arquivo"], "linha": int(m["linha"])} if m else None
    if not cs and acao:
        # Script lido do S3: a acao vira "count at path_N" (a linha fica dentro do path mascarado).
        m = re.search(r" at (path_\d+)$", acao)
        bruto = next((b for b, p in sanitizador.path_map.items() if m and p == m[1]), None)
        m2 = re.search(r"([^/\\]+):(\d+)$", bruto or "")
        if m2:
            return int(m2[2]) if m2[1].lower() == nome_script else None
    if not cs or not cs.get("arquivo"):
        return None
    return int(cs["linha"]) if _nome_real(cs["arquivo"], sanitizador) == nome_script else None


def vincular_ao_log(relatorio: dict, nome_script: str, sanitizador: Sanitizador) -> list[dict]:
    """Liga call sites/stages do log a linhas do script, quando o log aponta o arquivo."""
    nome = nome_script.lower()
    por_linha: dict[int, dict] = {}
    for c in relatorio.get("call_sites", []):
        n = _linha_do_call_site(c.get("acao"), c.get("call_site"), nome, sanitizador)
        if n:
            v = por_linha.setdefault(n, {"linha": n, "acoes": [], "num_jobs": 0,
                                         "duracao_total_s": 0.0, "stages": []})
            v["acoes"].append(c.get("acao"))
            v["num_jobs"] += c.get("num_jobs", 0)
            v["duracao_total_s"] = round(v["duracao_total_s"] + (c.get("duracao_total_s") or 0), 2)
    for s in relatorio.get("stages", []):
        o = s.get("origem") or {}
        n = _linha_do_call_site(o.get("acao"), o.get("call_site"), nome, sanitizador)
        if n:
            v = por_linha.setdefault(n, {"linha": n, "acoes": [o.get("acao")], "num_jobs": 0,
                                         "duracao_total_s": 0.0, "stages": []})
            v["stages"].append(s["stage_key"])
    return [por_linha[n] for n in sorted(por_linha)]


# --------------------------------------------------------------- montagem
def _corta(linha: str) -> str:
    return linha if len(linha) <= MAX_CHARS_LINHA else linha[: MAX_CHARS_LINHA - 3] + "..."


def montar_script(caminho: Path, relatorio: dict, sanitizador: Sanitizador,
                  modo: str | None = None) -> dict:
    """Le, limpa e mascara o script; devolve o bloco ``script`` do resumo.

    ``modo``: "completo", "trechos" ou None (automatico pelo tamanho).
    """
    caminho = Path(caminho)
    bruto = caminho.read_bytes()
    texto = bruto.decode("utf-8", errors="replace")
    linhas, n_comentarios = _sem_comentarios(texto)
    contagem = {"emails": 0, "segredos": 0}
    limpas = {str(i): _corta(_mascarar_linha(l, sanitizador, contagem))
              for i, l in enumerate(linhas, start=1) if l.strip()}

    vinculos = vincular_ao_log(relatorio, caminho.name, sanitizador)
    script = {
        "script_version": SCRIPT_VERSION,
        "nome": caminho.name,
        "sha256": hashlib.sha256(bruto).hexdigest(),
        "total_linhas": len(linhas),
        "sanitizacao": {"comentarios_removidos": n_comentarios, **contagem,
                        "paths_e_hosts": "mesmo mapa de pseudonimos do log (path_N, host_N)"},
        "vinculos_log": vinculos,
        "vinculos_status": (
            f"{len(vinculos)} linha(s) do script citadas por call sites do log."
            if vinculos else
            "Nenhum call site do log aponta para este arquivo (a DataFrame API do PySpark "
            "costuma registrar NativeMethodAccessorImpl.java:0). A ligacao stage -> linha "
            "do script NAO pode ser afirmada a partir do log."),
        "modo": "completo",
        "linhas": limpas,
    }
    if modo is None:
        modo = "completo" if len(limpas) <= MAX_LINHAS_COMPLETO else "trechos"
    return reduzir_script(script) if modo == "trechos" else _fechar(script)


def _fechar(script: dict) -> dict:
    script["linhas_enviadas"] = len(script["linhas"])
    script["criterio"] = (
        "Script inteiro, sem comentarios e linhas vazias. A chave de 'linhas' e o numero "
        "da linha no arquivo original." if script["modo"] == "completo" else
        f"Trechos: linhas citadas pelo log (+-{CONTEXTO_VINCULO}) e linhas com operacoes "
        f"Spark (+-{CONTEXTO_PALAVRA}). Ausencia de uma linha aqui NAO significa que ela "
        "nao exista no script.")
    return script


def reduzir_script(script: dict) -> dict:
    """completo -> trechos (deterministico; usado tambem no ajuste de tokens)."""
    if script.get("modo") == "trechos":
        return script
    todas = {int(k): v for k, v in script["linhas"].items()}
    escolhidas: set[int] = set()
    for v in script.get("vinculos_log", []):
        escolhidas.update(range(v["linha"] - CONTEXTO_VINCULO, v["linha"] + CONTEXTO_VINCULO + 1))
    for n, codigo in todas.items():
        if PALAVRAS_SPARK.search(codigo):
            escolhidas.update(range(n - CONTEXTO_PALAVRA, n + CONTEXTO_PALAVRA + 1))
    novo = {**script, "modo": "trechos",
            "linhas": {str(n): todas[n] for n in sorted(escolhidas) if n in todas}}
    return _fechar(novo)
