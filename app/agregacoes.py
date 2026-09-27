"""Agregacoes sobre os eventos: por stage, por call site e capacidade do cluster."""
from __future__ import annotations

import re
import statistics
from collections import defaultdict

from .parser import LogEvent


def mapear_stage_para_origem(eventos: list[LogEvent]) -> dict[str, dict]:
    mapa: dict[str, dict] = {}
    job_atual: dict | None = None

    for e in eventos:
        if e.event_type == "job_start":
            job_atual = {
                "job_id": e.data.get("job_id"),
                "acao": e.data.get("action"),
                "call_site": e.data.get("call_site"),
            }
        elif e.event_type == "stage_submit" and job_atual is not None:
            mapa.setdefault(e.data["stage_id"], job_atual)

    return mapa


def agregado_por_stage(eventos: list[LogEvent]) -> list[dict]:
    tasks: dict[str, dict] = {}
    for e in eventos:
        if e.event_type in ("task_start", "task_finish"):
            tasks.setdefault(e.data["tid"], {}).update(e.data)

    by_stage: dict[str, list[dict]] = defaultdict(list)
    for t in tasks.values():
        if "duration_ms" in t and "stage_id" in t:
            stage_id = t["stage_id"].split(".")[0]
            by_stage[stage_id].append(t)

    retries_por_stage: dict[str, int] = defaultdict(int)
    for t in tasks.values():
        task_id = t.get("task_id", "")
        stage_id_t = t.get("stage_id", "").split(".")[0]
        partes = task_id.split(".")
        if len(partes) == 2 and partes[1].isdigit() and int(partes[1]) > 0:
            retries_por_stage[stage_id_t] += 1

    origem_por_stage = mapear_stage_para_origem(eventos)

    # Tasks submetidas + descricao do RDD, por stage.
    submetidas: dict[str, dict] = {}
    for e in eventos:
        if e.event_type == "stage_tasks_submitted":
            submetidas[e.data["stage_id"]] = {
                "num_tasks_submetidas": int(e.data["num_tasks"]),
                "rdd_desc": e.data.get("rdd_desc"),
                "tipo_stage": e.data.get("stage"),
            }

    stage_meta = {}
    for e in eventos:
        if e.event_type == "stage_finish":
            stage_meta[e.data["stage_id"]] = {"duration_s": float(e.data["duration_s"]), "fonte": "stage_finish"}
        elif e.event_type == "taskset_removed" and e.data["stage_id"] not in stage_meta:
            stage_meta[e.data["stage_id"]] = {"duration_s": None, "fonte": "taskset_removed (sem duracao precisa)"}

    summary = []
    for stage_id, task_list in by_stage.items():
        durations = [int(t["duration_ms"]) for t in task_list]
        per_executor = defaultdict(list)
        for t in task_list:
            per_executor[t.get("executor", "?")].append(int(t["duration_ms"]))

        durations_sorted = sorted(durations)
        p95_idx = max(0, int(len(durations_sorted) * 0.95) - 1)
        median = statistics.median(durations)
        sub = submetidas.get(stage_id, {})

        summary.append({
            "stage_id": stage_id,
            "tipo_stage": sub.get("tipo_stage"),
            "rdd_desc": sub.get("rdd_desc"),
            "num_tasks": len(task_list),
            "num_tasks_submetidas": sub.get("num_tasks_submetidas"),
            "duration_s": stage_meta.get(stage_id, {}).get("duration_s"),
            "duration_fonte": stage_meta.get(stage_id, {}).get("fonte"),
            "task_duration_ms": {
                "min": min(durations),
                "max": max(durations),
                "mean": round(statistics.mean(durations), 1),
                "median": median,
                "p95": durations_sorted[p95_idx],
                "total": sum(durations),
            },
            "tasks_per_executor": {ex: len(v) for ex, v in per_executor.items()},
            "executor_duration_ms": {
                ex: {"mean": round(statistics.mean(v), 1), "total": sum(v)}
                for ex, v in per_executor.items()
            },
            "skew_ratio": round(max(durations) / median, 2) if median > 0 else None,
            "task_retries": retries_por_stage.get(stage_id, 0),
            "origem": origem_por_stage.get(stage_id),
        })

    return sorted(summary, key=lambda s: int(s["stage_id"]))


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
