from __future__ import annotations
import math
import re
import statistics
from collections import Counter, defaultdict
from .parser import LogEvent


def _dividir_id(valor: str) -> tuple[str, int]:
    """'249.1' -> ('249', 1); '249' -> ('249', 0)."""
    base, _, sufixo = str(valor).partition(".")
    return base, int(sufixo) if sufixo.isdigit() else 0


def _p95(ordenadas: list[int]) -> int:
    """Percentil 95 pelo metodo nearest-rank (sempre um valor observado)."""
    return ordenadas[max(0, math.ceil(0.95 * len(ordenadas)) - 1)]


def mapear_stage_para_origem(eventos: list[LogEvent]) -> dict[str, dict]:
    mapa: dict[str, dict] = {}
    job_atual: dict | None = None

    for e in eventos:
        if e.event_type in ("job_start", "map_stage_job_start"):
            job_atual = {
                "job_id": e.data.get("job_id"),
                "tipo_job": "map_stage" if e.event_type == "map_stage_job_start" else "job",
                "acao": e.data.get("action"),
                "call_site": e.data.get("call_site"),
            }
        elif e.event_type == "stage_submit" and job_atual is not None:
            mapa.setdefault(e.data["stage_id"], job_atual)

    return mapa


def _tasks_por_tid(eventos: list[LogEvent]) -> dict[str, dict]:
    """Junta start, finish e falha de cada task pelo TID (unico na aplicacao)."""
    tasks: dict[str, dict] = {}
    for e in eventos:
        if e.event_type in ("task_start", "task_finish"):
            tasks.setdefault(e.data["tid"], {}).update(e.data)
        elif e.event_type == "task_failed":
            t = tasks.setdefault(e.data["tid"], {})
            for chave in ("task_id", "stage_id", "executor"):
                t.setdefault(chave, e.data.get(chave))
            t["falhou"] = True
            t["motivo_falha"] = e.data.get("motivo")
    return tasks


def _metadados_por_tentativa(eventos: list[LogEvent], tentativas: dict[str, list[int]]) -> dict:
    """Associa submissao, duracao e status a cada tentativa (stage, attempt).

    As linhas do DAGScheduler trazem so o numero do stage, sem a tentativa.
    A associacao segue a ordem do log: a k-esima submissao/falha de um stage
    corresponde a k-esima tentativa; 'finished' corresponde a ultima.
    """
    meta: dict[tuple[str, int], dict] = {}
    submissoes: dict[str, list[dict]] = defaultdict(list)
    falhas: dict[str, list[dict]] = defaultdict(list)
    concluido: dict[str, float] = {}
    taskset_removido: set[tuple[str, int]] = set()

    for e in eventos:
        d = e.data
        if e.event_type == "stage_tasks_submitted":
            submissoes[d["stage_id"]].append(d)
        elif e.event_type == "stage_failed":
            falhas[d["stage_id"]].append(d)
        elif e.event_type == "stage_finish":
            concluido[d["stage_id"]] = float(d["duration_s"])
        elif e.event_type == "taskset_removed":
            taskset_removido.add((d["stage_id"], int(d["attempt"])))

    for stage, lista in tentativas.items():
        lista = sorted(lista)
        for posicao, attempt in enumerate(lista):
            sub = submissoes[stage][posicao] if posicao < len(submissoes[stage]) else {}
            falha = falhas[stage][posicao] if posicao < len(falhas[stage]) else None
            ultima = posicao == len(lista) - 1

            if falha is not None:
                status = "falhou"
                dur = float(falha["duration_s"]) if falha.get("duration_s") else None
                fonte = "stage_failed" if dur is not None else "stage_failed (sem duracao)"
            elif ultima and stage in concluido:
                status, dur, fonte = "concluida", concluido[stage], "stage_finish"
            elif (stage, attempt) in taskset_removido:
                status, dur, fonte = "concluida", None, "taskset_removed (sem duracao precisa)"
            else:
                status, dur, fonte = "desconhecido", None, None

            meta[(stage, attempt)] = {
                "num_tasks_submetidas": int(sub["num_tasks"]) if sub.get("num_tasks") else None,
                "rdd_desc": sub.get("rdd_desc"),
                "tipo_stage": sub.get("stage") or (falha or {}).get("stage"),
                "duration_s": dur,
                "duration_fonte": fonte,
                "status_tentativa": status,
                "motivo_falha_stage": (falha or {}).get("motivo"),
            }
    return meta


def agregado_por_stage(eventos: list[LogEvent]) -> list[dict]:
    """Uma entrada por TENTATIVA de stage (stage_id + stage_attempt)."""
    tasks = _tasks_por_tid(eventos)

    por_tentativa: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for t in tasks.values():
        if t.get("stage_id"):
            por_tentativa[_dividir_id(t["stage_id"])].append(t)

    tentativas: dict[str, list[int]] = defaultdict(list)
    for stage, attempt in por_tentativa:
        tentativas[stage].append(attempt)

    meta = _metadados_por_tentativa(eventos, tentativas)
    origem_por_stage = mapear_stage_para_origem(eventos)

    # Particoes ja vistas em tentativas anteriores, para contar reexecucoes.
    particoes_vistas: dict[str, set[str]] = defaultdict(set)

    summary = []
    for stage, attempt in sorted(por_tentativa, key=lambda k: (int(k[0]), k[1])):
        lista = por_tentativa[(stage, attempt)]
        concluidas = [t for t in lista if "duration_ms" in t]
        falhas = [t for t in lista if t.get("falhou")]

        # Usa a PARTICAO (campo "partition" do Starting task), nao o indice da task:
        # numa nova tentativa o Spark renumera as tasks a partir de 0.
        particoes = {t["partition"] for t in lista if t.get("partition") is not None}
        if attempt == 0:
            reexecutadas = 0
        elif particoes:
            reexecutadas = len(particoes & particoes_vistas[stage])
        else:
            reexecutadas = None  # padrao task_start sem o campo partition
        particoes_vistas[stage] |= particoes

        retries_mesma = sum(
            1 for t in lista if t.get("task_id") and _dividir_id(t["task_id"])[1] > 0
        )

        m = meta.get((stage, attempt), {})
        entrada = {
            "stage_id": stage,
            "stage_attempt": attempt,
            "stage_key": f"{stage}.{attempt}",
            "tentativas_do_stage": len(tentativas[stage]),
            "status_tentativa": m.get("status_tentativa"),
            "tipo_stage": m.get("tipo_stage"),
            "rdd_desc": m.get("rdd_desc"),
            "num_tasks": len(concluidas),
            "num_tasks_submetidas": m.get("num_tasks_submetidas"),
            "num_tasks_falhas": len(falhas),
            "falhas_por_motivo": dict(Counter(t.get("motivo_falha") or "?" for t in falhas)),
            "motivo_falha_stage": m.get("motivo_falha_stage"),
            "duration_s": m.get("duration_s"),
            "duration_fonte": m.get("duration_fonte"),
            "task_retries": retries_mesma + (reexecutadas or 0),
            "retries_mesma_tentativa": retries_mesma,
            "tasks_reexecutadas": reexecutadas,
            "origem": origem_por_stage.get(stage),
        }

        if concluidas:
            durations = sorted(int(t["duration_ms"]) for t in concluidas)
            median = statistics.median(durations)
            per_executor: dict[str, list[int]] = defaultdict(list)
            for t in concluidas:
                per_executor[t.get("executor", "?")].append(int(t["duration_ms"]))
            entrada.update({
                "task_duration_ms": {
                    "min": durations[0],
                    "max": durations[-1],
                    "mean": round(statistics.mean(durations), 1),
                    "median": median,
                    "p95": _p95(durations),
                    "total": sum(durations),
                },
                "tasks_per_executor": {ex: len(v) for ex, v in per_executor.items()},
                "executor_duration_ms": {
                    ex: {"mean": round(statistics.mean(v), 1), "total": sum(v)}
                    for ex, v in per_executor.items()
                },
                "skew_ratio": round(durations[-1] / median, 2) if median > 0 else None,
            })
        else:
            # Tentativa em que nenhuma task terminou (ex.: falhou logo no inicio).
            entrada.update({
                "task_duration_ms": {"min": None, "max": None, "mean": None,
                                     "median": None, "p95": None, "total": 0},
                "tasks_per_executor": {},
                "executor_duration_ms": {},
                "skew_ratio": None,
            })

        summary.append(entrada)

    return summary


def agregado_por_call_site(eventos: list[LogEvent]) -> list[dict]:
    jobs: dict[str, dict] = {}
    for e in eventos:
        if e.event_type == "job_start":
            jobs.setdefault(e.data["job_id"], {}).update({
                "acao": e.data.get("action"),
                "call_site": e.data.get("call_site"),
                "output_partitions": int(e.data.get("partitions", 0)),
            })
        elif e.event_type == "job_finish":
            jobs.setdefault(e.data["job_id"], {})["duration_s"] = float(e.data["duration_s"])

    agrupado: dict[str, dict] = {}
    for job_id, j in jobs.items():
        chave = j.get("acao") or "(sem call site)"
        alvo = agrupado.setdefault(chave, {
            "acao": chave,
            "call_site": j.get("call_site"),
            "num_jobs": 0,
            "duracao_total_s": 0.0,
            "output_partitions_total": 0,
            "job_ids": [],
        })
        alvo["num_jobs"] += 1
        alvo["duracao_total_s"] += j.get("duration_s", 0.0)
        alvo["output_partitions_total"] += j.get("output_partitions", 0)
        alvo["job_ids"].append(job_id)

    saida = []
    for v in agrupado.values():
        v["duracao_total_s"] = round(v["duracao_total_s"], 2)
        v["duracao_media_s"] = round(v["duracao_total_s"] / v["num_jobs"], 3)
        saida.append(v)
    return sorted(saida, key=lambda x: -x["duracao_total_s"])


def capacidade_cluster(eventos: list[LogEvent], stage_summary: list[dict],
                       duracao_total_s: float | None) -> dict:
    max_exec = None
    cores = None
    for e in eventos:
        if e.event_type == "max_executors":
            max_exec = int(e.data["max_executors"])
        elif e.event_type == "resource_profile":
            m = re.search(r"cores\s*->\s*name:\s*cores,\s*amount:\s*(\d+)", e.data.get("recursos", ""))
            if m:
                cores = int(m.group(1))

    executores_vistos = set()
    for s in stage_summary:
        executores_vistos.update(s["tasks_per_executor"].keys())
    executores_vistos.discard("?")

    tempo_task_s = sum(s["task_duration_ms"]["total"] for s in stage_summary) / 1000
    slots = (max_exec * cores) if (max_exec and cores) else None
    capacidade_s = (slots * duracao_total_s) if (slots and duracao_total_s) else None

    return {
        "max_executors": max_exec,
        "cores_por_executor": cores,
        "slots": slots,
        "executores_com_tasks": sorted(executores_vistos, key=lambda x: int(x) if x.isdigit() else 0),
        "tempo_total_task_s": round(tempo_task_s, 1),
        "capacidade_total_s": round(capacidade_s, 1) if capacidade_s else None,
        "ocupacao_pct": round(100 * tempo_task_s / capacidade_s, 1) if capacidade_s else None,
        "fonte": "slots = effectiveMaxExecutors x cores por executor; ocupacao = tempo de task / capacidade",
    }