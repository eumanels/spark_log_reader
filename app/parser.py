"""Leitura do log bruto e conversao em eventos estruturados."""
from __future__ import annotations

from dataclasses import dataclass, field

from .padroes import (
    CALL_SITE_PATTERN,
    LINHAS_BASE,
    METODOS_INTERNOS,
    PADROES_DE_EVENTOS,
    TMP_SPARK_PATTERN,
)
from .sanitizador import Sanitizador


@dataclass
class LogEvent:
    ts: str
    level: str
    component: str
    event_type: str
    data: dict = field(default_factory=dict)


def normalizar_acao(action: str) -> str:
    return TMP_SPARK_PATTERN.sub("", action.strip())


def extrair_call_site(action: str) -> dict | None:
    m = CALL_SITE_PATTERN.match(normalizar_acao(action))
    if not m:
        return None
    metodo = m.group("metodo")
    return {
        "metodo": metodo,
        "arquivo": m.group("arquivo"),
        "linha": int(m.group("linha")),
        "interno": metodo in METODOS_INTERNOS,
    }


def parse_log(linhas, sanitizador: Sanitizador | None = None) -> list[LogEvent]:
    if sanitizador is None:
        sanitizador = Sanitizador()

    eventos: list[LogEvent] = []
    pendente_stack_trace: LogEvent | None = None

    for linha_bruta in linhas:
        linha = linha_bruta.rstrip("\n")
        m = LINHAS_BASE.match(linha.strip())

        if not m:
            if pendente_stack_trace is not None and linha.strip():
                pendente_stack_trace.data.setdefault("stack_trace", [])
                pendente_stack_trace.data["stack_trace"].append(
                    sanitizador.mask_text(linha.strip())
                )
            continue

        ts, level, component, msg = m.group("ts", "level", "component", "msg")
        pendente_stack_trace = None

        matched = False
        for event_type, pattern in PADROES_DE_EVENTOS.items():
            em = pattern.match(msg)
            if em:
                data = em.groupdict()
                if "host" in data and data["host"]:
                    data["host_masked"] = sanitizador.mask_host(data.pop("host"))
                if event_type == "job_start" and data.get("action"):
                    data["action"] = normalizar_acao(data["action"])
                    call_site = extrair_call_site(data["action"])
                    if call_site:
                        data["call_site"] = call_site
                if event_type == "stage_tasks_submitted" and data.get("rdd_desc"):
                    data["rdd_desc"] = normalizar_acao(data["rdd_desc"])
                eventos.append(LogEvent(ts, level, component, event_type, data))
                matched = True
                break

        if not matched and level in ("WARN", "ERROR"):
            evento = LogEvent(ts, level, component, "raw_warn_error",
                              {"msg": sanitizador.mask_text(msg)})
            eventos.append(evento)
            pendente_stack_trace = evento

    return eventos
