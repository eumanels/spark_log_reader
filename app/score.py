from __future__ import annotations
from .padroes import PESOS_SCORE

SCORE_VERSION = "1.0"


def _faixa(score: int) -> str:
    if score >= PESOS_SCORE["faixa_boa_min"]:
        return "boa"
    if score >= PESOS_SCORE["faixa_atencao_min"]:
        return "atencao"
    return "critica"


def calcular_score(r: dict) -> dict:
    p = PESOS_SCORE
    base = {"versao": SCORE_VERSION, "pesos": dict(p)}

    cons = r.get("consistencia") or {}
    if cons and not cons.get("ok", True):
        return {**base, "score": None, "faixa": None, "descontos": [],
                "motivo": "Consistencia com erro: os numeros de entrada nao sao confiaveis."}

    descontos: list[dict] = []

    def descontar(criterio: str, pontos: float, teto: float, evidencia, calculo: str) -> None:
        aplicado = min(pontos, teto)
        if aplicado > 0:
            descontos.append({"criterio": criterio, "pontos": round(aplicado, 1),
                              "evidencia": evidencia, "calculo": calculo,
                              "limitado_pelo_teto": pontos > teto})

    # 1. Sucesso da execucao
    status = r.get("status_final") or []
    exit_code = status[0]["data"].get("exit_code") if status else None
    if exit_code != "0":
        descontar("execucao_sem_sucesso", p["execucao_sem_sucesso"], p["execucao_sem_sucesso"],
                  {"exit_code": exit_code}, "exit_code ausente ou diferente de 0")

    # 2. Executores perdidos
    n = (r.get("executor_lost") or {}).get("total", 0)
    descontar("executores_perdidos", n * p["por_executor_perdido"], p["teto_executores_perdidos"],
              {"executor_lost_total": n}, f"{n} x {p['por_executor_perdido']}")

    # 3. Falhas de stage
    n = (r.get("stage_failures") or {}).get("total", 0)
    descontar("falhas_de_stage", n * p["por_falha_de_stage"], p["teto_falhas_de_stage"],
              {"stage_failures_total": n}, f"{n} x {p['por_falha_de_stage']}")

    # 4. Retentativas de task
    n = r.get("task_retries_total", 0)
    pontos = (n // p["retentativas_por_ponto"]) * 1
    descontar("retentativas_de_task", pontos, p["teto_retentativas"],
              {"task_retries_total": n}, f"{n} // {p['retentativas_por_ponto']}")

    # 5. Achados: cada regra conta uma vez, pela maior severidade
    peso_sev = {"ALTA": p["achado_alta"], "MEDIA": p["achado_media"],
                "BAIXA": p["achado_baixa"], "INFO": 0}
    por_regra: dict[str, str] = {}
    for a in r.get("oportunidades", []):
        atual = por_regra.get(a["regra"])
        if atual is None or peso_sev[a["severidade"]] > peso_sev[atual]:
            por_regra[a["regra"]] = a["severidade"]
    pontos = sum(peso_sev[s] for s in por_regra.values())
    descontar("achados", pontos, p["teto_achados"],
              {regra: sev for regra, sev in sorted(por_regra.items()) if peso_sev[sev] > 0},
              " + ".join(f"{regra}={peso_sev[sev]}" for regra, sev in sorted(por_regra.items())
                         if peso_sev[sev] > 0) or "0")

    total = sum(d["pontos"] for d in descontos)
    score = max(0, round(100 - total))
    return {**base, "score": score, "faixa": _faixa(score), "descontos": descontos,
            "total_descontado": round(total, 1)}