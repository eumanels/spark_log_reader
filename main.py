import re
import pandas as pd

"""
Parser de log stderr do Spark (driver) - extrai eventos estruturados
para tabela, usando templates de regex por componente/tipo de evento.

Baseado nas linhas de exemplo fornecidas (SparkContext, DAGScheduler,
MemoryStore, BlockManagerInfo, TaskSchedulerImpl, TaskSetManager).

IMPORTANTE: os templates de GC, shuffle e spill abaixo são ilustrativos -
o log de exemplo não continha essas linhas. Antes de confiar neles,
calibre com trechos reais do seu stderr que mostrem esses eventos
(procure por "GC", "shuffle", "spilling" nos seus 3 logs de referência)
e ajuste as regex.

Uso:
    from spark_log_parser import parse_log_file, build_masked_summary
    df = parse_log_file("caminho/para/log_stderr.txt")
    df_seguro = build_masked_summary(df)
"""

# Regex do cabeçalho de toda linha do log
LINE_HEADER = re.compile(
    r"^(?P<date>\d{2}/\d{2}/\d{2}) (?P<time>\d{2}:\d{2}:\d{2}) "
    r"(?P<level>\w+) (?P<component>\S+): (?P<message>.*)$"
)

# Templates de evento: (nome_evento, regex_do_componente, regex_da_mensagem)
EVENT_PATTERNS = [
    ("job_start", "SparkContext", re.compile(
        r"Starting job: (?P<job_desc>.+) with (?P<num_partitions>\d+) output partitions"
    )),
    ("job_got", "DAGScheduler", re.compile(
        r"Got job (?P<job_id>\d+) \((?P<job_desc>.+)\) with (?P<num_partitions>\d+) output partitions"
    )),
    ("final_stage", "DAGScheduler", re.compile(
        r"Final stage: ResultStage (?P<stage_id>\d+) \((?P<stage_desc>.+)\)"
    )),
    ("stage_submitting", "DAGScheduler", re.compile(
        r"Submitting (?P<stage_type>\w+Stage) (?P<stage_id>\d+) \((?P<rdd_desc>.+)\), which has no missing parents"
    )),
    ("tasks_submitting", "DAGScheduler", re.compile(
        r"Submitting (?P<num_tasks>\d+) missing tasks from (?P<stage_type>\w+Stage) (?P<stage_id>\d+)"
    )),
    ("taskset_adding", "TaskSchedulerImpl", re.compile(
        r"Adding task set (?P<stage_id>\d+)\.(?P<attempt>\d+) with (?P<num_tasks>\d+) tasks"
    )),
    ("task_start", "TaskSetManager", re.compile(
        r"Starting task (?P<task_idx>\d+)\.(?P<attempt>\d+) in stage (?P<stage_id>\d+)\.(?P<stage_attempt>\d+) "
        r"\(TID (?P<tid>\d+)\) \(\[(?P<host>[0-9a-fA-F:\.]+)\](?::\d+)?, "
        r"executor (?P<executor_id>\d+), partition (?P<partition>\d+), "
        r"(?P<locality>\w+), (?P<bytes>\d+) bytes\)"
    )),
    # ilustrativo - confirmar formato real do "Finished task" no seu log
    ("task_finish", "TaskSetManager", re.compile(
        r"Finished task (?P<task_idx>\d+)\.(?P<attempt>\d+) in stage (?P<stage_id>\d+)\.(?P<stage_attempt>\d+) "
        r"\(TID (?P<tid>\d+)\) in (?P<duration_ms>\d+) ms"
    )),
    ("broadcast_stored", "MemoryStore", re.compile(
        r"Block broadcast_(?P<broadcast_id>\d+) stored as values in memory "
        r"\(estimated size (?P<size_val>[\d.]+) (?P<size_unit>\w+), free (?P<free_val>[\d.]+) (?P<free_unit>\w+)\)"
    )),
    ("block_added", "BlockManagerInfo", re.compile(
        r"Added broadcast_(?P<broadcast_id>\d+)_piece\d+ in memory on "
        r"\[(?P<host>[0-9a-fA-F:\.]+)\]:(?P<port>\d+) "
        r"\(size: (?P<size_val>[\d.]+) (?P<size_unit>\w+), free: (?P<free_val>[\d.]+) (?P<free_unit>\w+)\)"
    )),
    # ilustrativos - calibrar com linhas reais de GC/shuffle/spill/falha
    ("gc_info", "Executor", re.compile(r"GC.*took (?P<duration_ms>\d+) ms")),
    ("shuffle_write", "ShuffleMapTask|ShuffleWriter", re.compile(
        r"[Ss]huffle.*written.*?(?P<bytes>\d+)"
    )),
    ("spill_event", "ExternalSorter|Spillable", re.compile(
        r"[Ss]pilling.*?(?P<bytes>\d+)"
    )),
    ("executor_lost", "TaskSchedulerImpl|DAGScheduler", re.compile(
        r"[Ee]xecutor (?P<executor_id>\d+) lost"
    )),
    ("stage_failed", "DAGScheduler", re.compile(
        r"Stage (?P<stage_id>\d+) \(.*\) failed"
    )),
]

# componentes que são só ruído (não carregam métrica de performance)
NOISE_COMPONENTS = {"CodeGenerator"}


def mask_host(host: str, host_map: dict) -> str:
    """Substitui IP real por ID pseudônimo consistente (host_1, host_2...)."""
    if host not in host_map:
        host_map[host] = f"host_{len(host_map) + 1}"
    return host_map[host]


def parse_log_file(path: str) -> pd.DataFrame:
    rows = []
    host_map = {}

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            header_match = LINE_HEADER.match(line)

            if not header_match:
                # linha de continuação (ex: stack trace multi-linha)
                if rows:
                    rows[-1].setdefault("continuation", []).append(line)
                continue

            h = header_match.groupdict()
            component = h["component"]
            message = h["message"]

            if component in NOISE_COMPONENTS:
                continue

            event_row = {
                "lineno": lineno,
                "timestamp": f"{h['date']} {h['time']}",
                "level": h["level"],
                "component": component,
                "event_type": "unclassified",
                "raw_message": None,
            }

            matched = False
            for event_type, comp_pattern, regex in EVENT_PATTERNS:
                if not re.fullmatch(comp_pattern, component):
                    continue
                m = regex.search(message)
                if m:
                    event_row["event_type"] = event_type
                    event_row.update(m.groupdict())
                    matched = True
                    break

            if not matched:
                # guarda a mensagem crua só para revisão local (Camada 1)
                event_row["raw_message"] = message

            if event_row.get("host"):
                event_row["host_masked"] = mask_host(event_row["host"], host_map)

            rows.append(event_row)

    return pd.DataFrame(rows)


def build_masked_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Camada 2: remove host real e mensagem crua - o que pode ir para o
    assistente de IA."""
    drop_cols = [c for c in ("host", "raw_message", "continuation") if c in df.columns]
    return df.drop(columns=drop_cols)


if __name__ == "__main__":
    import sys
    df = parse_log_file(r'C:\Users\Pichau\Documents\spark_log_reader\log_REF_TRACKING_sucesso.txt')
    print(f"Total de eventos: {len(df)}")
    print(df["event_type"].value_counts())
    build_masked_summary(df)
    df

