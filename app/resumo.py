from __future__ import annotations
import re
from collections import Counter

RESUMO_VERSION = "1.2"
PERFIS = {
    "padrao": {"top_stages": 10, "top_call_sites": 10, "top_grupos_warn": 20,
               "max_detalhes_falha": 20, "max_chars_texto": 200, "max_linhas_stack": 3,
               "achados_com_texto": True, "achados_enxutos": False,
               "stages_so_com_falha": False, "call_sites": True, "warn_com_exemplo": True},
    "compacto": {"top_stages": 5, "top_call_sites": 5, "top_grupos_warn": 8,
                 "max_detalhes_falha": 5, "max_chars_texto": 120, "max_linhas_stack": 0,
                 "achados_com_texto": False, "achados_enxutos": False,
                 "stages_so_com_falha": False, "call_sites": True, "warn_com_exemplo": True},
    "minimo": {"top_stages": 0, "top_call_sites": 0, "top_grupos_warn": 5,
               "max_detalhes_falha": 3, "max_chars_texto": 100, "max_linhas_stack": 0,
               "achados_com_texto": False, "achados_enxutos": True,
               "stages_so_com_falha": True, "call_sites": False, "warn_com_exemplo": False},
}
ORDEM_PERFIS = ["padrao", "compacto", "minimo"]

TOP_STAGES = PERFIS["padrao"]["top_stages"]
TOP_CALL_SITES = PERFIS["padrao"]["top_call_sites"]
TOP_GRUPOS_WARN = PERFIS["padrao"]["top_grupos_warn"]
MAX_DETALHES_FALHA = PERFIS["padrao"]["max_detalhes_falha"]
MAX_CHARS_TEXTO = PERFIS["padrao"]["max_chars_texto"]
MAX_LINHAS_STACK = PERFIS["padrao"]["max_linhas_stack"]

_NUMERO = re.compile(r"\d+")


def _corta(texto: str | None, limite: int = MAX_CHARS_TEXTO) -> str | None:
    if texto is None:
        return None
    return texto if len(texto) <= limite else texto[: limite - 3] + "..."


def _sem_nulos(d: dict) -> dict:
    """Remove chaves vazias para economizar espaco no prompt."""
    return {k: v for k, v in d.items() if v is not None and v != {}}


def _stage_compacto(s: dict) -> dict:
    d = s.get("task_duration_ms") or {}
    o = s.get("origem") or {}
    return _sem_nulos({
        "stage_key": s["stage_key"],
        "tipo_stage": s.get("tipo_stage"),
        "status_tentativa": s.get("status_tentativa"),
        "duration_s": s.get("duration_s"),
        "num_tasks": s.get("num_tasks"),
        "num_tasks_submetidas": s.get("num_tasks_submetidas"),
        "task_ms_mediana": d.get("median"),
        "task_ms_p95": d.get("p95"),
        "task_ms_max": d.get("max"),
        "skew_ratio": s.get("skew_ratio"),
        "num_tasks_falhas": s.get("num_tasks_falhas"),
        "falhas_por_motivo": s.get("falhas_por_motivo") or None,
        "task_retries": s.get("task_retries"),
        "origem_acao": o.get("acao"),
        "origem_tipo_job": o.get("tipo_job"),
        "origem_interna_do_engine": bool((o.get("call_site") or {}).get("interno")) if o else None,
    })


def _selecionar_stages(stages: list[dict], achados: list[dict], top: int = TOP_STAGES,
                       so_com_falha: bool = False) -> tuple[list[dict], int]:
    por_duracao = sorted(stages, key=lambda s: -(s.get("duration_s") or 0))
    if so_com_falha:
        escolhidos = {s["stage_key"] for s in stages
                      if s.get("num_tasks_falhas") or s.get("status_tentativa") == "falhou"}
    else:
        citados = {a["evidencia"].get("stage_key") for a in achados if "stage_key" in a["evidencia"]}
        escolhidos = {s["stage_key"] for s in por_duracao[:top]} | citados
    lista = [s for s in por_duracao if s["stage_key"] in escolhidos]
    return [_stage_compacto(s) for s in lista], len(stages) - len(lista)


def _selecionar_call_sites(call_sites: list[dict], achados: list[dict],
                           top: int = TOP_CALL_SITES) -> tuple[list[dict], int]:
    citados = {a["evidencia"].get("acao") for a in achados if a["regra"] == "acao_repetida"}
    escolhidos = call_sites[:top] + [c for c in call_sites[top:] if c["acao"] in citados]
    compactos = [{
        "acao": c["acao"],
        "num_jobs": c["num_jobs"],
        "duracao_total_s": c["duracao_total_s"],
        "duracao_media_s": c["duracao_media_s"],
        "interno_do_engine": bool((c.get("call_site") or {}).get("interno")),
    } for c in escolhidos]
    return compactos, len(call_sites) - len(escolhidos)


def agrupar_warnings(itens: list[dict], cfg: dict | None = None) -> tuple[list[dict], int]:
    """Agrupa raw_warn_error por (nivel, componente, mensagem com numeros -> N)."""
    cfg = cfg or PERFIS["padrao"]
    max_chars, top = cfg["max_chars_texto"], cfg["top_grupos_warn"]
    contagem: Counter = Counter()
    primeiro: dict = {}
    for e in itens:
        msg = (e.get("data") or {}).get("msg", "")
        chave = (e.get("level"), e.get("component"), _NUMERO.sub("N", msg)[:max_chars])
        contagem[chave] += 1
        primeiro.setdefault(chave, e)

    grupos = []
    for (nivel, componente, template), n in contagem.most_common(top):
        ex = primeiro[(nivel, componente, template)]
        g = {
            "nivel": nivel,
            "componente": componente,
            "template": template,
            "ocorrencias": n,
            "primeiro_ts": ex.get("ts"),
        }
        if cfg.get("warn_com_exemplo", True):
            g["exemplo"] = _corta(ex["data"].get("msg"), max_chars)
        stack = ex["data"].get("stack_trace")
        if stack and cfg["max_linhas_stack"]:
            g["exemplo_stack"] = [_corta(s, max_chars) for s in stack[:cfg["max_linhas_stack"]]]
        grupos.append(g)
    return grupos, max(0, len(contagem) - top)


def _falhas(r: dict, stages: list[dict], cfg: dict) -> dict:
    n, mc = cfg["max_detalhes_falha"], cfg["max_chars_texto"]
    por_motivo: Counter = Counter()
    for s in stages:
        por_motivo.update(s.get("falhas_por_motivo") or {})
    lost = (r.get("executor_lost") or {}).get("detalhes", [])
    fails = (r.get("stage_failures") or {}).get("detalhes", [])
    return {
        "executor_lost_total": (r.get("executor_lost") or {}).get("total", 0),
        "executor_lost": [{"ts": e.get("ts"), "executor": e["data"].get("executor"),
                           "motivo": _corta(e["data"].get("reason"), mc)} for e in lost[:n]],
        "stage_failures_total": (r.get("stage_failures") or {}).get("total", 0),
        "stage_failures": [{"ts": e.get("ts"), "stage_id": e["data"].get("stage_id"),
                            "tipo_stage": e["data"].get("stage"),
                            "motivo": _corta(e["data"].get("motivo"), mc)} for e in fails[:n]],
        "tasks_falhas_por_motivo": dict(por_motivo.most_common()),
        "task_retries_total": r.get("task_retries_total", 0),
    }


def tamanho_para_prompt(resumo: dict) -> int:
    """Bytes do resumo serializado sem indentacao (como vai no prompt)."""
    import json
    return len(json.dumps(resumo, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _achado_para_ia(a: dict, i: int, cfg: dict) -> dict:
    base = {"id": f"A{i}", **a}
    if not cfg["achados_com_texto"]:
        # Ficam so os fatos: regra, severidade, titulo, evidencia e limiar.
        # Hipoteses e sugestoes sao texto que a IA refaz.
        for chave in ("hipoteses", "investigar", "opcoes_se_confirmado"):
            base.pop(chave, None)
    if cfg["achados_enxutos"]:
        base.pop("titulo", None)
        base.pop("limiar_usado", None)
        ev = dict(base.get("evidencia") or {})
        if isinstance(ev.get("origem"), dict):
            ev["origem_acao"] = ev.pop("origem").get("acao")
        ev.pop("rdd_desc", None)
        base["evidencia"] = ev
    return base


def montar_resumo_ia(r: dict, perfil: str = "padrao") -> dict:
    """Recebe o relatorio COMPLETO e JA SANITIZADO; devolve o resumo para a IA."""
    cfg = PERFIS[perfil]
    meta = r.get("meta") or {}
    achados = r.get("oportunidades", [])
    stages = r.get("stages", [])
    stages_c, stages_omitidos = _selecionar_stages(stages, achados, cfg["top_stages"],
                                                   cfg["stages_so_com_falha"])
    if cfg["call_sites"]:
        calls_c, calls_omitidos = _selecionar_call_sites(r.get("call_sites", []), achados,
                                                         cfg["top_call_sites"])
    else:
        calls_c, calls_omitidos = [], len(r.get("call_sites", []))
    warns, grupos_omitidos = agrupar_warnings(r.get("raw_warn_error", []), cfg)
    limiares = meta.get("limiares")
    if perfil != "padrao" and isinstance(limiares, dict):
        limiares = {"deteccao": limiares.get("deteccao")}
    indisponiveis = r.get("metricas_nao_disponiveis")
    if perfil != "padrao" and isinstance(indisponiveis, dict):
        indisponiveis = sorted(indisponiveis)
    cap = r.get("capacidade_cluster") or {}
    status = r.get("status_final") or []
    cons = r.get("consistencia") or {}

    return {
        "resumo_version": RESUMO_VERSION,
        "fonte": {
            "log_nome": meta.get("log_nome"),
            "parser_version": meta.get("parser_version"),
            "schema_version": meta.get("schema_version"),
            "tipo_de_log": "stderr do driver Spark, nivel INFO",
            "perfil_do_resumo": perfil,
        },
        "limiares": limiares,
        "contexto": {
            "app_name": (r.get("contexto") or {}).get("app_name"),
            "spark_version": (r.get("contexto") or {}).get("spark_version"),
            "snapshots_delta_carregados": (r.get("contexto") or {}).get("snapshots_delta_carregados"),
        },
        "execucao": {
            "duracao_total_s": (r.get("duracao_total_execucao") or {}).get("duration_s"),
            "exit_code": status[0]["data"].get("exit_code") if status else None,
            "slots_estimados": cap.get("slots"),
            "executores_com_tasks": len(cap.get("executores_com_tasks") or []),
            "max_executors": cap.get("max_executors"),
            "cores_por_executor": cap.get("cores_por_executor"),
            "tempo_total_task_s": cap.get("tempo_total_task_s"),
            "ocupacao_pct": cap.get("ocupacao_pct"),
            "total_stages": len(stages),
            "total_jobs": sum(c.get("num_jobs", 0) for c in r.get("call_sites", [])),
        },
        "falhas": _falhas(r, stages, cfg),
        "achados": [_achado_para_ia(a, i, cfg) for i, a in enumerate(achados, start=1)],
        "stages": stages_c,
        "call_sites": calls_c,
        "warnings_e_erros": warns,
        "metricas_nao_disponiveis": indisponiveis,
        "qualidade_dos_dados": {
            "consistencia_ok": cons.get("ok"),
            "alertas": [{"codigo": a["codigo"], "severidade": a["severidade"],
                         "mensagem": a["mensagem"]} for a in cons.get("alertas", [])],
        },
        "omitidos": {
            "stages": stages_omitidos,
            "call_sites": calls_omitidos,
            "grupos_de_warning": grupos_omitidos,
            "criterio": f"top {cfg['top_stages']} stages e top {cfg['top_call_sites']} call sites por "
                        f"duracao, mais os citados em achados; top {cfg['top_grupos_warn']} grupos de warning.",
        },
    }