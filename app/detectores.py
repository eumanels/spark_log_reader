"""Regras que transformam as agregacoes em achados de otimizacao."""
from __future__ import annotations

from .padroes import LIMIARES
from .parser import LogEvent


def _achado(regra, severidade, titulo, evidencia, acao):
    return {
        "regra": regra,
        "severidade": severidade,
        "titulo": titulo,
        "evidencia": evidencia,
        "acao_sugerida": acao,
    }


def detectar_oportunidades(eventos: list[LogEvent], stage_summary: list[dict],
                           call_sites: list[dict], capacidade: dict) -> list[dict]:
    achados = []

    # 1. Window sem partitionBy
    janelas = [e for e in eventos if e.event_type == "window_sem_particao"]
    if janelas:
        achados.append(_achado(
            "window_sem_particao", "ALTA",
            "Window function sem partitionBy",
            {"ocorrencias": len(janelas), "primeiro_ts": janelas[0].ts, "ultimo_ts": janelas[-1].ts},
            "Todo o dataset e movido para uma unica particao. Adicionar partitionBy "
            "na chave de negocio; se a numeracao precisa ser global, trocar por "
            "zipWithIndex/monotonically_increasing_id.",
        ))

    # 2. Stage explodido: muitas tasks, cada uma trivial
    for s in stage_summary:
        n = s["num_tasks_submetidas"] or s["num_tasks"]
        if n >= LIMIARES["stage_explodido_tasks"] and \
           s["task_duration_ms"]["median"] < LIMIARES["stage_explodido_mediana_ms"]:
            achados.append(_achado(
                "stage_explodido", "ALTA",
                f"Stage {s['stage_id']} com {n} tasks triviais",
                {
                    "stage_id": s["stage_id"],
                    "num_tasks": n,
                    "mediana_ms": s["task_duration_ms"]["median"],
                    "tempo_total_task_s": round(s["task_duration_ms"]["total"] / 1000, 1),
                    "rdd_desc": s["rdd_desc"],
                },
                "Particoes pequenas demais: quase todo o tempo e overhead de "
                "agendamento. Revisar coalesce/repartition antes do write e o "
                "minPartitionSize do AQE.",
            ))

    # 3. AQE coalescendo para muito abaixo do alvo
    alvos = [e for e in eventos if e.event_type == "aqe_coalesce_target"]
    suspeitos = [
        e for e in alvos
        if int(e.data["actual_bytes"]) > 0
        and int(e.data["advisory_bytes"]) / int(e.data["actual_bytes"]) >= LIMIARES["aqe_razao_alvo"]
    ]
    if suspeitos:
        exemplo = suspeitos[0].data
        achados.append(_achado(
            "aqe_alvo_reduzido", "MEDIA",
            "AQE coalescendo shuffle muito abaixo do advisory",
            {
                "ocorrencias": len(suspeitos),
                "advisory_bytes": int(exemplo["advisory_bytes"]),
                "actual_bytes": int(exemplo["actual_bytes"]),
                "min_partition_bytes": int(exemplo["min_bytes"]),
                "razao": round(int(exemplo["advisory_bytes"]) / int(exemplo["actual_bytes"]), 1),
            },
            "Ajustar spark.sql.adaptive.coalescePartitions.minPartitionSize e "
            "avaliar coalescePartitions.parallelismFirst=false.",
        ))

    # 4. Acao repetida = laco no driver
    for cs in call_sites:
        if cs["num_jobs"] >= LIMIARES["acao_repetida_min_jobs"]:
            interno = bool(cs.get("call_site") and cs["call_site"].get("interno"))
            achados.append(_achado(
                "acao_repetida", "MEDIA" if not interno else "BAIXA",
                f"{cs['num_jobs']} jobs disparados por {cs['acao'][:70]}",
                {
                    "acao": cs["acao"],
                    "num_jobs": cs["num_jobs"],
                    "duracao_total_s": cs["duracao_total_s"],
                    "interno_do_engine": interno,
                },
                "Acao dentro de laco: cada iteracao reexecuta o plano. Vetorizar "
                "em join/agregacao unica, ou ao menos cachear o DataFrame base."
                if not interno else
                "Chamada interna do Delta/engine repetida (ex: history/metadata). "
                "Cachear o resultado no driver em vez de reconsultar.",
            ))

    # 5. Skew de task
    for s in stage_summary:
        # O piso absoluto e essencial: sem ele, um stage de 9 tasks com
        # mediana de 100 ms e um outlier de 3 s vira "skew 29x" - razao
        # enorme, impacto nenhum.
        if s["skew_ratio"] and s["skew_ratio"] >= LIMIARES["skew_ratio"] \
           and s["num_tasks"] >= LIMIARES["skew_min_tasks"] \
           and s["task_duration_ms"]["max"] >= LIMIARES["skew_min_max_ms"]:
            achados.append(_achado(
                "skew_task", "MEDIA",
                f"Stage {s['stage_id']} com distribuicao desbalanceada",
                {
                    "stage_id": s["stage_id"],
                    "skew_ratio": s["skew_ratio"],
                    "max_ms": s["task_duration_ms"]["max"],
                    "median_ms": s["task_duration_ms"]["median"],
                    "num_tasks": s["num_tasks"],
                },
                "Chave de join/particionamento concentrada. Avaliar salting ou "
                "skewJoin do AQE.",
            ))

    # 6. Fragmentacao: maioria dos stages com 1 task
    if stage_summary:
        um_task = [s for s in stage_summary if s["num_tasks"] == 1]
        pct = 100 * len(um_task) / len(stage_summary)
        if pct >= LIMIARES["stage_1_task_pct"]:
            achados.append(_achado(
                "fragmentacao_stages", "BAIXA",
                "Maioria dos stages roda com uma unica task",
                {"stages_com_1_task": len(um_task), "total_stages": len(stage_summary), "pct": round(pct, 1)},
                "Pipeline fragmentado em muitas acoes pequenas: o custo e latencia "
                "de driver, nao processamento. Consolidar transformacoes.",
            ))

    # 7. Small files no scan
    scans = [e for e in eventos if e.event_type == "scan_bin_packing"]
    small = [
        e for e in scans
        if int(e.data["num_files"]) >= LIMIARES["small_files_min"]
        and int(e.data["max_split_bytes"]) <= int(e.data["open_cost_bytes"])
    ]
    if small:
        achados.append(_achado(
            "small_files", "MEDIA",
            "Scan com muitos arquivos pequenos",
            {"ocorrencias": len(small),
             "max_arquivos_em_um_scan": max(int(e.data["num_files"]) for e in small)},
            "Custo de abertura domina a leitura. Rodar OPTIMIZE/compactacao na "
            "tabela de origem.",
        ))

    # 8. Ocupacao baixa do cluster
    if capacidade.get("ocupacao_pct") is not None and \
       capacidade["ocupacao_pct"] < LIMIARES["ocupacao_baixa_pct"]:
        achados.append(_achado(
            "ocupacao_baixa", "MEDIA",
            "Cluster ocioso em boa parte da execucao",
            {k: capacidade[k] for k in ("slots", "tempo_total_task_s", "capacidade_total_s", "ocupacao_pct")},
            "Tempo gasto fora de task (planejamento, metadado, commit). Reduzir "
            "numero de acoes e conferir o teto de executores.",
        ))

    ordem = {"ALTA": 0, "MEDIA": 1, "BAIXA": 2}
    return sorted(achados, key=lambda a: ordem[a["severidade"]])
