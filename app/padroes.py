from __future__ import annotations
import re

# ---------------------------------------------------------------------------
# Linha base de todo evento estruturado
# ---------------------------------------------------------------------------
LINHAS_BASE = re.compile(
    r"^(?P<ts>\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) "
    r"(?P<level>TRACE|DEBUG|INFO|WARN|ERROR|FATAL) "
    r"(?P<component>[\w$.]+): "
    r"(?P<msg>.*)$"
)

# ---------------------------------------------------------------------------
# Linhas SEM cabecalho (continuacao de um evento multilinha)
# ---------------------------------------------------------------------------
# Stack trace Java ou traceback Python.
PARECE_STACK = re.compile(
    r"^(?:\s*at\s"                                   # \tat org.apache...
    r"|\s*\.\.\. \d+ more"                          # ... 12 more
    r"|\s*Caused by:"                                # Caused by: ...
    r"|\s*Suppressed:"
    r"|Traceback \(most recent call last\)"          # Python
    r"|\s*File \"[^\"]+\", line \d+"                 #   File "x.py", line 10
    r"|During handling of the above exception"
    r"|[\w$.]+(?:Exception|Error|Throwable)\b"        # java.lang.X: / ValueError:
    r")"
)
# Plano logico/fisico quebrado em varias linhas (ex.: AdaptiveSparkPlan ... +- Sort).
CONTINUACAO_PLANO = re.compile(r"^\s*(?:[+:|]\s*)*[+:]-\s")

LIMITE_STACK_LINHAS = 50        # linhas de stack guardadas por evento
LIMITE_CONTINUACAO_CHARS = 20000  # texto maximo juntado a uma mensagem multilinha
LIMITE_RDD_DESC_CHARS = 300

# Formato real do log4j do Spark: yy/MM/dd (NAO dd/MM/yy).
FORMATO_TS = "%y/%m/%d %H:%M:%S"

# ---------------------------------------------------------------------------
# Mascara de dados sensiveis - IPs, hostnames EC2, paths S3 e ARNs
# Os padroes so encontram CANDIDATOS; o Sanitizador confirma cada IP com o
# modulo ipaddress antes de mascarar (numeros, horarios e UUIDs nao sao IPs).
# ---------------------------------------------------------------------------
IPV6_COLCHETES_PATTERN = re.compile(r"\[([0-9a-fA-F:.]+)\](?::\d+)?")
IPV4_PATTERN = re.compile(r"(?<![\w.])(\d{1,3}(?:\.\d{1,3}){3})(?::\d+)?(?![\w.])")
IPV6_SOLTO_PATTERN = re.compile(r"(?<![\w:.\[])((?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4})(?![\w:.\]])")
HOST_EC2_PATTERN = re.compile(r"\bip-(\d{1,3}(?:-\d{1,3}){3})(?:\.[\w-]+)*(?::\d+)?")
PATH_PATTERN = re.compile(r"s3[an]?://[^\s,)\"']+")
ARN_PATTERN = re.compile(r"arn:aws[\w-]*:[^\s,)\"']+")
TMP_SPARK_PATTERN = re.compile(r"/tmp/spark-[0-9a-f\-]{8,}/")

# ---------------------------------------------------------------------------
# Call site
# ---------------------------------------------------------------------------
CALL_SITE_PATTERN = re.compile(r"^(?P<metodo>\S+) at (?P<arquivo>[^:]+):(?P<linha>\d+)$")

METODOS_INTERNOS = {
    "getHistory",
    "$anonfun$recordDeltaOperationInternal$1",
    "$anonfun$withThreadLocalCaptured$1",
}

# ---------------------------------------------------------------------------
# Eventos reconhecidos no log
# ---------------------------------------------------------------------------
PADROES_DE_EVENTOS = {
    "job_start": re.compile(r"^Got job (?P<job_id>\d+) \((?P<action>.*?)\) with (?P<partitions>\d+) output partitions$"),
    # Jobs de "map stage" (AQE materializando um shuffle). Nao tem linha de fim
    # propria: terminam quando o ShuffleMapStage deles termina.
    "map_stage_job_start": re.compile(
        r"^Got map stage job (?P<job_id>\d+) \((?P<action>.*?)\) with (?P<partitions>\d+) output partitions$"
    ),
    "job_finish": re.compile(r"^Job (?P<job_id>\d+) finished: .*?, took (?P<duration_s>[\d.]+) s$"),
    # Marcador de fim que o DAGScheduler grava para TODO job (inclusive os que
    # nao ganham a linha "finished: ..., took X s").
    "job_concluido": re.compile(r"^Job (?P<job_id>\d+) is finished\. Cancelling"),
    "job_failed": re.compile(r"^Job (?P<job_id>\d+) failed: .*?, took (?P<duration_s>[\d.]+) s$"),
    "stage_submit": re.compile(r"^Submitting (?P<stage>ResultStage|ShuffleMapStage) (?P<stage_id>\d+)"),
    "stage_finish": re.compile(r"^(?P<stage>ResultStage|ShuffleMapStage) (?P<stage_id>\d+) \(.*?\) finished in (?P<duration_s>[\d.]+) s$"),
    "taskset_removed": re.compile(r"^Removed TaskSet (?P<stage_id>\d+)\.(?P<attempt>\d+), whose tasks have all completed"),
    "stage_failed": re.compile(
        r"^(?P<stage>ResultStage|ShuffleMapStage) (?P<stage_id>\d+) \(.*?\) failed"
        r"(?: in (?P<duration_s>[\d.]+) s)?(?: due to (?P<motivo>.+))?"
    ),
    "task_failed": re.compile(
        r"^Lost task (?P<task_id>[\d.]+) in stage (?P<stage_id>[\d.]+) \(TID (?P<tid>\d+)\) "
        r"\((?P<host>\S+) executor (?P<executor>\w+)\): (?P<motivo>[\w.$]+)"
    ),
    "task_start": re.compile(
        r"^Starting task (?P<task_id>[\d.]+) in stage (?P<stage_id>[\d.]+) \(TID (?P<tid>\d+)\) "
        r"\((?P<host>[^,]+), executor (?P<executor>\d+)(?:, partition (?P<partition>\d+))?"
    ),
    "task_finish": re.compile(
        r"^Finished task (?P<task_id>[\d.]+) in stage (?P<stage_id>[\d.]+) \(TID (?P<tid>\d+)\) "
        r"in (?P<duration_ms>\d+) ms on (?P<host>[^\s(]+) \(executor (?P<executor>\d+)\)"
    ),
    "broadcast_stored": re.compile(
        r"^Block broadcast_(?P<broadcast_id>\d+)(?:_piece\d+)? stored as (?:values|bytes) in memory "
        r"\(estimated size (?P<size_val>[\d.]+) (?P<size_unit>\w+),(?: actual size: [\d.]+ \w+,)? "
        r"free (?P<free_val>[\d.]+) (?P<free_unit>\w+)\)$"
    ),
    "block_added": re.compile(
        r"^Added broadcast_(?P<broadcast_id>\d+)_piece\d+ in memory on "
        r"(?P<host>\[[0-9a-fA-F:\.]+\]:\d+) "
        r"\(size: (?P<size_val>[\d.]+) (?P<size_unit>\w+), free: (?P<free_val>[\d.]+) (?P<free_unit>\w+)\)$"
    ),
    "shuffle_map_output_request": re.compile(r"^Asked to send map output locations for shuffle (?P<shuffle_id>\d+)$"),
    "window_sem_particao": re.compile(r"^No Partition Defined for Window operation"),
    "stage_tasks_submitted": re.compile(
        r"^Submitting (?P<num_tasks>\d+) missing tasks from "
        r"(?P<stage>ShuffleMapStage|ResultStage) (?P<stage_id>\d+) \((?P<rdd_desc>.*?)\)"
    ),
    "aqe_coalesce_target": re.compile(
        r"^For shuffle\((?P<shuffle_id>[\d,\s]+)\), advisory target size: (?P<advisory_bytes>\d+), "
        r"actual target size (?P<actual_bytes>\d+), minimum partition size: (?P<min_bytes>\d+)$"
    ),
    "scan_bin_packing": re.compile(
        r"^Planning scan with bin packing, max size: (?P<max_split_bytes>\d+) bytes, "
        r"open cost is considered as scanning (?P<open_cost_bytes>\d+) bytes, "
        r"number of split files: (?P<num_files>\d+), prefetch: (?P<prefetch>\w+)$"
    ),
    "max_executors": re.compile(r"^Using effectiveMaxExecutors = (?P<max_executors>\d+)"),
    "resource_profile": re.compile(r"^Default ResourceProfile created, executor resources: (?P<recursos>.*)$"),
    "delta_snapshot": re.compile(
        r"^Loading version (?P<version>\d+) starting from checkpoint version (?P<checkpoint>\d+)\.?$"
    ),
    "app_name": re.compile(r"^Submitted application: (?P<app_name>.+)$"),
    "spark_version": re.compile(r"^Running Spark version (?P<spark_version>\S+)$"),
    "codegen": re.compile(r"^Code generated in (?P<duration_ms>[\d.]+) ms$"),

    "executor_backlog_request": re.compile(r"^Requesting (?P<num_requested>\d+) new executors because tasks are backlogged"),
    "executor_registered": re.compile(r"^New executor (?P<executor>\d+) has registered \(new total is (?P<total>\d+)\)$"),
    "executor_not_found": re.compile(r"^No executor found for (?P<host>[0-9a-fA-F:\.]+)$"),
    "executor_lost": re.compile(r"^Lost executor (?P<executor>\d+) on (?P<host>\[[^\]]+\]|[^\s:]+): (?P<reason>.+)$"),
    "app_final_status": re.compile(r"^SparkContext is stopping with exitCode (?P<exit_code>\d+)\.?$"),
}

# ---------------------------------------------------------------------------
# Limiares das regras de deteccao
# ---------------------------------------------------------------------------
LIMIARES = {
    "stage_explodido_tasks": 500,
    "stage_explodido_mediana_ms": 100,
    "aqe_razao_alvo": 8,
    "acao_repetida_min_jobs": 5,
    "skew_ratio": 3.0,
    "skew_min_tasks": 8,
    "skew_min_max_ms": 5000,
    "stage_1_task_pct": 40,
    "small_files_min": 50,
    "ocupacao_baixa_pct": 60,
}

# ---------------------------------------------------------------------------
# Pesos do Score de Qualidade (app/score.py) - documentados no README
# ---------------------------------------------------------------------------
PESOS_SCORE = {
    "execucao_sem_sucesso": 40,
    "por_executor_perdido": 5,
    "teto_executores_perdidos": 20,
    "por_falha_de_stage": 5,
    "teto_falhas_de_stage": 20,
    "retentativas_por_ponto": 10,      # 1 ponto a cada 10 retentativas
    "teto_retentativas": 10,
    "achado_alta": 10,
    "achado_media": 5,
    "achado_baixa": 2,
    "teto_achados": 30,
    "faixa_boa_min": 80,
    "faixa_atencao_min": 50,
}