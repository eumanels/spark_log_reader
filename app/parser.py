from __future__ import annotations
from dataclasses import dataclass, field
from .padroes import (
    CALL_SITE_PATTERN,
    CONTINUACAO_PLANO,
    LIMITE_CONTINUACAO_CHARS,
    LIMITE_RDD_DESC_CHARS,
    LIMITE_STACK_LINHAS,
    LINHAS_BASE,
    METODOS_INTERNOS,
    PADROES_DE_EVENTOS,
    PARECE_STACK,
    TMP_SPARK_PATTERN,
)
from .sanitizador import Sanitizador

NIVEIS_DE_ALERTA = ("WARN", "ERROR", "FATAL")


@dataclass
class LogEvent:
    ts: str
    level: str
    component: str
    event_type: str
    data: dict = field(default_factory=dict)


@dataclass
class _Registro:
    """Uma linha com cabecalho + as linhas sem cabecalho que a seguem."""
    ts: str
    level: str
    component: str
    msg: str
    continuacao: list[str] = field(default_factory=list)
    chars_continuacao: int = 0
    n_continuacao: int = 0
    stack: list[str] = field(default_factory=list)
    n_stack: int = 0


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


def _novas_estatisticas() -> dict:
    return {
        "linhas_total": 0,
        "linhas_com_cabecalho": 0,
        "linhas_continuacao": 0,
        "linhas_stack": 0,
        "linhas_avulsas": 0,
        "exemplos_avulsas": [],
        "stack_traces_truncados": 0,
        "excecoes_sem_cabecalho": 0,
    }


def _classificar(linha: str, atual: _Registro | None) -> str:
    if PARECE_STACK.match(linha):
        return "stack"
    # Dentro de um stack, linhas indentadas (codigo do traceback Python) continuam o stack.
    if atual is not None and atual.n_stack and linha[:1].isspace():
        return "stack"
    if atual is not None and not atual.n_stack and CONTINUACAO_PLANO.match(linha):
        return "continuacao"
    return "avulsa"


def _casar(msg: str) -> tuple[str, dict] | None:
    for event_type, pattern in PADROES_DE_EVENTOS.items():
        em = pattern.match(msg)
        if em:
            return event_type, em.groupdict()
    return None


def _anexar_stack(evento: LogEvent, reg: _Registro, linhas: list[str],
                  sanitizador: Sanitizador, stats: dict) -> None:
    evento.data["stack_trace"] = [sanitizador.mask_text(s) for s in linhas]
    total = len(linhas) + (reg.n_stack - len(reg.stack))
    if total > len(linhas):
        evento.data["stack_trace_truncado"] = True
        evento.data["stack_trace_total_linhas"] = total
        stats["stack_traces_truncados"] += 1


def _emitir(reg: _Registro, eventos: list[LogEvent], sanitizador: Sanitizador, stats: dict) -> None:
    # 1) Tenta casar a mensagem completa (com as linhas de continuacao); se nao
    #    casar, tenta so a primeira linha (regex ancoradas em $).
    casado = None
    if reg.continuacao:
        completa = reg.msg + " " + " ".join(s.strip() for s in reg.continuacao)
        casado = _casar(completa)
    if casado is None:
        casado = _casar(reg.msg)

    evento: LogEvent | None = None
    if casado:
        event_type, data = casado
        if data.get("host"):
            data["host_masked"] = sanitizador.mask_host(data.pop("host"))
        if event_type in ("job_start", "map_stage_job_start") and data.get("action"):
            data["action"] = normalizar_acao(data["action"])
            call_site = extrair_call_site(data["action"])
            if call_site:
                data["call_site"] = call_site
        if event_type == "stage_tasks_submitted" and data.get("rdd_desc"):
            data["rdd_desc"] = normalizar_acao(data["rdd_desc"])[:LIMITE_RDD_DESC_CHARS]
        if reg.n_continuacao:
            data["linhas_continuacao"] = reg.n_continuacao
        evento = LogEvent(reg.ts, reg.level, reg.component, event_type, data)
        eventos.append(evento)
    elif reg.level in NIVEIS_DE_ALERTA:
        data = {"msg": sanitizador.mask_text(reg.msg)}
        if reg.n_continuacao:
            data["linhas_continuacao"] = reg.n_continuacao
        evento = LogEvent(reg.ts, reg.level, reg.component, "raw_warn_error", data)
        eventos.append(evento)

    if not reg.stack:
        return

    # 2) Stack trace: fica no proprio evento quando ele e WARN/ERROR/FATAL.
    if evento is not None and reg.level in NIVEIS_DE_ALERTA:
        _anexar_stack(evento, reg, reg.stack, sanitizador, stats)
        return

    # 3) Stack depois de uma linha INFO/DEBUG (ex.: traceback do PySpark
    #    impresso direto no stderr): vira um evento proprio para nao sumir.
    excecao = LogEvent(
        reg.ts, "ERROR", "stderr", "raw_warn_error",
        {"msg": sanitizador.mask_text(reg.stack[0].strip()),
         "origem": f"excecao sem cabecalho log4j (apos linha {reg.level} {reg.component})"},
    )
    _anexar_stack(excecao, reg, reg.stack[1:], sanitizador, stats)
    eventos.append(excecao)
    stats["excecoes_sem_cabecalho"] += 1


def parse_log(linhas, sanitizador: Sanitizador | None = None,
              estatisticas: dict | None = None) -> list[LogEvent]:
    """Converte as linhas do stderr em eventos.

    ``estatisticas`` (opcional) e preenchido com contagens da leitura:
    linhas com/sem cabecalho, stack, continuacao, avulsas e exemplos.
    """
    if sanitizador is None:
        sanitizador = Sanitizador()
    stats = estatisticas if estatisticas is not None else {}
    stats.update(_novas_estatisticas())

    eventos: list[LogEvent] = []
    atual: _Registro | None = None

    for linha_bruta in linhas:
        stats["linhas_total"] += 1
        linha = linha_bruta.rstrip("\r\n")
        if not linha.strip():
            continue

        m = LINHAS_BASE.match(linha.strip())
        if m:
            if atual is not None:
                _emitir(atual, eventos, sanitizador, stats)
            atual = _Registro(*m.group("ts", "level", "component", "msg"))
            stats["linhas_com_cabecalho"] += 1
            continue

        tipo = _classificar(linha, atual)

        if tipo == "stack":
            stats["linhas_stack"] += 1
            if atual is None:
                # Stack antes de qualquer cabecalho (ex.: traceback no inicio do arquivo).
                atual = _Registro("", "INFO", "stderr", "")
            atual.n_stack += 1
            if len(atual.stack) < LIMITE_STACK_LINHAS:
                atual.stack.append(linha.rstrip())
        elif tipo == "continuacao":
            stats["linhas_continuacao"] += 1
            atual.n_continuacao += 1
            if atual.chars_continuacao < LIMITE_CONTINUACAO_CHARS:
                atual.continuacao.append(linha)
                atual.chars_continuacao += len(linha)
        else:
            stats["linhas_avulsas"] += 1
            if len(stats["exemplos_avulsas"]) < 3:
                stats["exemplos_avulsas"].append(sanitizador.mask_text(linha.strip())[:200])

    if atual is not None:
        _emitir(atual, eventos, sanitizador, stats)

    return eventos