from __future__ import annotations
import re
from .modelos import AnaliseIA

CAMPOS_IDENTIFICADORES = ("stage_key", "id", "acao", "codigo", "template", "regra")
_INEXISTENTE = object()


def _segmentos(caminho: str) -> list[tuple[str, list[str]]]:
    """'a.b[x].c' -> [('a', []), ('b', ['x']), ('c', [])]; pontos dentro de [] sao preservados."""
    partes, atual, prof = [], "", 0
    for ch in caminho:
        if ch == "[":
            prof += 1
        elif ch == "]":
            prof -= 1
        if ch == "." and prof == 0:
            partes.append(atual)
            atual = ""
        else:
            atual += ch
    partes.append(atual)
    saida = []
    for p in partes:
        m = re.fullmatch(r"([^\[\]]*)((?:\[[^\]]*\])*)", p)
        if not m:
            raise ValueError(f"caminho mal formado: {caminho}")
        saida.append((m.group(1), re.findall(r"\[([^\]]*)\]", m.group(2))))
    return saida


def _item(lista: list, seletor: str):
    for item in lista:
        if isinstance(item, dict) and any(str(item.get(c)) == seletor for c in CAMPOS_IDENTIFICADORES):
            return item
    if seletor.isdigit() and int(seletor) < len(lista):
        return lista[int(seletor)]
    return _INEXISTENTE


def resolver_caminho(dados, caminho: str):
    """Valor no caminho, ou ``_INEXISTENTE``."""
    atual = dados
    try:
        segmentos = _segmentos(caminho.strip())
    except ValueError:
        return _INEXISTENTE
    for nome, seletores in segmentos:
        if nome:
            if not isinstance(atual, dict) or nome not in atual:
                return _INEXISTENTE
            atual = atual[nome]
        for sel in seletores:
            if not isinstance(atual, list):
                return _INEXISTENTE
            atual = _item(atual, sel.strip())
            if atual is _INEXISTENTE:
                return _INEXISTENTE
    return atual


def _iguais(citado, real) -> bool:
    if citado == real:
        return True
    try:
        return float(citado) == float(real)
    except (TypeError, ValueError):
        return str(citado).strip() == str(real).strip()


def conferir(caminho: str, citado, resumo: dict) -> dict:
    real = resolver_caminho(resumo, caminho)
    if real is _INEXISTENTE:
        return {"caminho": caminho, "citado": citado, "real": None, "status": "inexistente"}
    status = "confere" if _iguais(citado, real) else "diverge"
    return {"caminho": caminho, "citado": citado, "real": real, "status": status}


def conferir_analise(analise: AnaliseIA, resumo: dict) -> dict:
    """Confere todas as evidencias (classificacao + gargalos) e totaliza."""
    classif = [conferir(e.caminho, e.valor, resumo) for e in analise.evidencias_classificacao]
    gargalos = {g.id: [conferir(e.caminho, e.valor, resumo) for e in g.evidencias] for g in analise.gargalos}
    todas = classif + [c for lista in gargalos.values() for c in lista]
    total = {s: sum(1 for c in todas if c["status"] == s) for s in ("confere", "diverge", "inexistente")}
    return {"classificacao": classif, "gargalos": gargalos, "totais": total,
            "pct_confere": round(100 * total["confere"] / len(todas), 1) if todas else None}


def classificacao_esperada(resumo: dict) -> str:
    """Aplica o criterio de classificacao do prompt (analise_v1), na mesma ordem."""
    if not (resumo.get("qualidade_dos_dados") or {}).get("consistencia_ok", True):
        return "inconclusiva"
    ex = resumo.get("execucao") or {}
    if ex.get("exit_code") != "0":
        return "com_falha"
    fa = resumo.get("falhas") or {}
    sev = {a.get("severidade") for a in resumo.get("achados", [])}
    if (fa.get("executor_lost_total") or fa.get("stage_failures_total") or fa.get("task_retries_total")
            or sev & {"ALTA", "MEDIA"}):
        return "degradada"
    return "saudavel"


def conferir_classificacao(analise: AnaliseIA, resumo: dict) -> dict:
    esperada = classificacao_esperada(resumo)
    return {"esperada": esperada, "obtida": analise.classificacao.value,
            "confere": esperada == analise.classificacao.value}