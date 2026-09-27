"""Contexto, duracao, falhas e montagem do JSON final."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import datetime as dt_datetime

from .agregacoes import agregado_por_call_site, agregado_por_stage, capacidade_cluster
from .detectores import detectar_oportunidades
from .padroes import FORMATO_TS
from .parser import LogEvent


def resumo_eventos_esparsos(eventos: list[LogEvent]) -> dict:
    contagem = defaultdict(int)
    for e in eventos:
        contagem[e.event_type] += 1
    return dict(sorted(contagem.items(), key=lambda kv: -kv[1]))


def contexto_aplicacao(eventos: list[LogEvent]) -> dict:
    ctx = {"app_name": None, "spark_version": None, "tabelas_delta": []}
    tabelas = []
    for e in eventos:
        if e.event_type == "app_name":
            ctx["app_name"] = e.data["app_name"]
        elif e.event_type == "spark_version":
            ctx["spark_version"] = e.data["spark_version"]
        elif e.event_type == "delta_snapshot":
            tabelas.append({"version": int(e.data["version"]), "checkpoint": int(e.data["checkpoint"])})
    ctx["snapshots_delta_carregados"] = len(tabelas)
    return ctx


def eventos_de_falha(eventos: list[LogEvent]) -> dict:
    stage_failures = [asdict(e) for e in eventos if e.event_type == "stage_failed"]
    executor_lost = [asdict(e) for e in eventos if e.event_type == "executor_lost"]
    return {
        "stage_failures": {"total": len(stage_failures), "detalhes": stage_failures},
        "executor_lost": {"total": len(executor_lost), "detalhes": executor_lost},
    }


def duracao_total_execucao(eventos: list[LogEvent]) -> dict:
    timestamps = []
    for e in eventos:
        try:
            # O log4j do Spark grava yy/MM/dd, nao dd/MM/yy.
            timestamps.append(dt_datetime.strptime(e.ts, FORMATO_TS))
        except ValueError:
            continue

    if not timestamps:
        return {"duration_s": None, "fonte": "nenhum timestamp pode ser interpretado"}

    inicio, fim = min(timestamps), max(timestamps)
    return {
        "duration_s": (fim - inicio).total_seconds(),
        "inicio": inicio.strftime(FORMATO_TS),
        "fim": fim.strftime(FORMATO_TS),
        "fonte": "diferenca entre o primeiro e o ultimo timestamp do log",
    }


def metricas_nao_disponiveis() -> dict:
    return {
        "gc_time_ratio": {
            "valor": None,
            "motivo": "Log em nivel INFO padrao, sem eventos de GC verboso (flag -verbose:gc nao habilitada).",
        },
        "shuffle_read_write_bytes": {
            "valor": None,
            "motivo": "Log expoe apenas sinal indireto de shuffle (advisory/actual target size), nao bytes lidos/escritos reais.",
        },
        "spill_memoria_disco": {
            "valor": None,
            "motivo": "Nao ha eventos de spill no log neste nivel de verbosidade.",
        },
        "result_size_driver": {
            "valor": None,
            "motivo": "Result Size por task so existe no event log (SparkListenerTaskEnd). "
                      "Sem ele, o risco de collect e estimado por numero de particoes, nao por bytes.",
        },
    }


def montar_relatorio(parsed: list[LogEvent]) -> dict:
    """Roda todas as agregacoes e regras e devolve o dicionario final."""
    stage_summary = agregado_por_stage(parsed)
    call_sites = agregado_por_call_site(parsed)
    duracao_total = duracao_total_execucao(parsed)
    capacidade = capacidade_cluster(parsed, stage_summary, duracao_total.get("duration_s"))
    oportunidades = detectar_oportunidades(parsed, stage_summary, call_sites, capacidade)
    falhas = eventos_de_falha(parsed)

    return {
        "contexto": contexto_aplicacao(parsed),
        "duracao_total_execucao": duracao_total,
        "capacidade_cluster": capacidade,
        "oportunidades": oportunidades,
        "call_sites": call_sites,
        "stages": stage_summary,
        "task_retries_total": sum(s["task_retries"] for s in stage_summary),
        "stage_failures": falhas["stage_failures"],
        "executor_lost": falhas["executor_lost"],
        "eventos_esparsos": resumo_eventos_esparsos(parsed),
        "metricas_nao_disponiveis": metricas_nao_disponiveis(),
        "status_final": [asdict(e) for e in parsed if e.event_type == "app_final_status"],
        "raw_warn_error": [asdict(e) for e in parsed if e.event_type == "raw_warn_error"],
    }


def imprimir_resumo(output: dict) -> None:
    capacidade = output["capacidade_cluster"]
    oportunidades = output["oportunidades"]
    print(f"Aplicacao : {output['contexto']['app_name']}")
    print(f"Duracao   : {output['duracao_total_execucao']['duration_s']} s")
    print(f"Ocupacao  : {capacidade['ocupacao_pct']}% de {capacidade['slots']} slots")
    print(f"Achados   : {len(oportunidades)}")
    for a in oportunidades:
        print(f"  [{a['severidade']:<5}] {a['titulo']}")
