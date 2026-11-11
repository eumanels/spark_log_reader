# --- BIBLIOTECAS PYSPARK --- #
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from utils.zoro import *
import logging
import boto3
from tempfile import TemporaryFile
from urllib.parse import urlparse
from delta.tables import DeltaTable
from utils_maxpar.funcoes import valida_atualizacao_dependencias
from utils_maxpar import classe_decisor_incidentes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- VARIAVEIS --- #
eng = 'JOSE_LIMA'
path_s3 = "s3://"
bucket = "{{repositorioS3}}-{{ambiente}}"
bucket_mlops = "{{repositorioS3mlops}}/{{complemento_CD}}"
nome_tabela = "REP_CICLO_PRESTADOR_ASSISTENCIA"

# --- INICIALIZA INSTÂNCIA DE SPARKSESSION COM DELTA --- #
configs = [
    ("spark.databricks.delta.optimizeWrite.enabled", "true"),
    ("spark.databricks.delta.autoCompact.enabled", "true"),
    ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
    ("spark.sql.adaptive.enabled", "true"),
    ("spark.sql.adaptive.skewJoin.enabled", "true")
]
dl = DeltaLake(app_name=nome_tabela, configs=configs)
spark = dl.build()
decisor = classe_decisor_incidentes.DecisorIncidentes(spark)

# --- PATHS --- #
trusted_zone = "TRUSTED"
report_zone = "REPORT"
maxpar = "MAXPAR"
a24h = "A24H"
produto = "PRODUTO"

path_bucket = f"{path_s3}{bucket}/"
path_bucket_mlops = f"{path_s3}{bucket_mlops}/"
schema_tru_aci = f"{trusted_zone}/{maxpar}/{a24h}/"
schema_rep_prod = f"{report_zone}/{maxpar}/{produto}/"

# REPORT
tru_aci_topico_path = path_bucket + schema_tru_aci + "TRU_GENESIS_ACIONAMENTO"
tru_aci_prestador_path = path_bucket + schema_tru_aci + "TRU_PRESTADOR_GENESIS_ACIONAMENTO"

# NOVA TABELA
path_table = path_bucket + schema_rep_prod + nome_tabela

# --------- VALIDA INGESTÃO --------- #
lista_path_dependencias = [
    tru_aci_topico_path,
    tru_aci_prestador_path,
]

if "{{ambiente}}" == "prod-us-1":
    decisor.decisor_de_incidentes(lista_path_dependencias)
    valida_atualizacao_dependencias(tru_aci_topico_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_aci_prestador_path, valida_ingestao=False)

# --- COLUNAS NECESSÁRIAS --- #
cols_aci_topico = [
    "COD_PROTOCOLO_ATENDIMENTO",
    "DAT_ACEITE_ACIONAMENTO",
    "TPO_SITUACAO_ATENDIMENTO",
    "DSC_PRESTADOR_ACIONAMENTO",
    "COD_PRESTADOR_BASE",
    "TPO_COBERTURA",
]

cols_aci_prestador = [
    "COD_IDENTIFICADOR",
    "DSC_NOME_FANTASIA",
]

# --- LEITURAS --- #
#Compensacao de 3 horas validada para os horarios gravados no Lake, considerando a sessao Spark em UTC. Mantem a janela de 140 minutos.
agora_compensado = current_timestamp() - expr("INTERVAL 3 HOURS")

acionamento_topico = (spark.read.format("delta")
                      .load(tru_aci_topico_path)
                      .select(cols_aci_topico)
                      .filter(col("TPO_SITUACAO_ATENDIMENTO").isin("ACEITO", "NAORIGEM", "NA ORIGEM", "NODESTINO", "NO DESTINO", "CONCLUÍDO", "CONCLUIDO", "FINALIZADO")
                              & (col("DAT_ACEITE_ACIONAMENTO") >= agora_compensado - expr("INTERVAL 140 MINUTES"))
                              & (col("DAT_ACEITE_ACIONAMENTO") <= agora_compensado)
                              & (col("TPO_COBERTURA") == "REBOQUE LEVE")))

acionamento_prestador = spark.read.format("delta").load(tru_aci_prestador_path).select(cols_aci_prestador)

# --- SCRIPT --- #
tabela_join = acionamento_topico.join(
    acionamento_prestador, acionamento_topico["DSC_PRESTADOR_ACIONAMENTO"] == acionamento_prestador["DSC_NOME_FANTASIA"], "left"
)

tabela = tabela_join.groupBy(
    col("COD_IDENTIFICADOR").alias("COD_PRESTADOR"),
    col("COD_PRESTADOR_BASE"),
).agg(
    count_distinct("COD_PROTOCOLO_ATENDIMENTO").alias("QTD_CICLO")
)

# --- PADRONIZAÇÃO DE TIPAGEM E TRATAMENTO DE NULOS --- #
col_cast_tabela = {
    "COD_PRESTADOR": ["string", "0"],
    "COD_PRESTADOR_BASE": ["string", "0"],
    "QTD_CICLO": ["long", 0],
}

exprs = []
for coluna in tabela.columns:
    dtype = col_cast_tabela[coluna][0]
    default_null = col_cast_tabela[coluna][1]
    coluna_expr = col(coluna)
    is_nulo_ou_vazio = coluna_expr.isNull() | (trim(coluna_expr.cast("string")) == "")

    if dtype in ("timestamp", "date"):
        expr = (
            when(is_nulo_ou_vazio, lit(default_null).cast(dtype))
            .otherwise(coluna_expr.cast(dtype))
            .alias(coluna)
        )
    elif dtype is not None:
        expr = (
            when(is_nulo_ou_vazio, lit(default_null).cast(dtype))
            .otherwise(coluna_expr.cast(dtype))
            .cast(dtype)
            .alias(coluna)
        )
    else:
        expr = coluna_expr.alias(coluna)

    exprs.append(expr)

tabela = tabela.select(*exprs).select(
    "COD_PRESTADOR",
    "COD_PRESTADOR_BASE",
    "QTD_CICLO",
)

# --- ESCRITA DA TABELA --- #
log.info("INÍCIO ESCRITA DA REFINED NO LAKE")

tabela.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_table)

log.info("ESCRITA DA REFINED NO LAKE EXECUTADA COM SUCESSO")

delta_table = DeltaTable.forPath(spark, path_table)
delta_table.vacuum(72)
delta_table.generate("symlink_format_manifest")
log.info("Manifesto criado com sucesso!")

# --- READ E WRITE MLOPS --- #
report_mlops = spark.read.format("delta").load(path_table)
path_table_mlops = f"{path_bucket_mlops}{schema_rep_prod}" + nome_tabela

log.info("INÍCIO ESCRITA DA REPORT NA ESTEIRA DO MLOPS")
nome_arquivo_mlops = "qtd_ciclo.json"
destino_mlops = urlparse(f"{path_table_mlops}/{nome_arquivo_mlops}")

with TemporaryFile(mode="w+b") as arquivo_json:
    for linha in report_mlops.toJSON().toLocalIterator():
        arquivo_json.write((linha + "\n").encode("utf-8"))
    arquivo_json.seek(0)
    boto3.client("s3").upload_fileobj(
        arquivo_json,
        destino_mlops.netloc,
        destino_mlops.path.lstrip("/"),
        ExtraArgs={"ContentType": "application/json"},
    )

log.info("ESCRITA DA REPORT NA ESTEIRA DO MLOPS EXECUTADA COM SUCESSO")
 