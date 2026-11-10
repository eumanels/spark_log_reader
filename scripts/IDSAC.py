# --- BIBLIOTECAS E DEPENDÊNCIAS --- #
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.zoro import *
from datetime import datetime, timezone, timedelta
import logging
from delta.tables import DeltaTable
from utils_maxpar.funcoes import valida_atualizacao_dependencias
from utils_maxpar import classe_decisor_incidentes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# --- VARIÁVEIS / PARAMETRIZAÇÃO --- #
eng = "JOSE_LIMA"
path_s3 = "s3://"
bucket = "{{repositorioS3}}-{{ambiente}}"
nome_tabela = "REF_IDSAC_ASSISTENCIA"

# --- INICIALIZAÇÃO SPARKSESSION (DELTA) --- #
configs = [
    ("spark.databricks.delta.optimizeWrite.enabled", "true"),
    ("spark.databricks.delta.autoCompact.enabled", "true"),
    ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
]
dl = DeltaLake(app_name=nome_tabela, configs=configs)
spark = dl.build()
decisor = classe_decisor_incidentes.DecisorIncidentes(spark)

trusted_zone = "TRUSTED"
report_zone = "REPORT"
refined_zone = "REFINED"
maxpar = "MAXPAR"
atendimento = "ATENDIMENTO"
a24h = "A24H"
master = "MASTER"

# PATHs
path_bucket = f"{path_s3}{bucket}/"
schema_rep_att = f"{report_zone}/{maxpar}/{atendimento}/"
schema_rep_att_a24h = f"{report_zone}/{maxpar}/{a24h}/"
schema_ref_a24h = f"{refined_zone}/{maxpar}/{a24h}/"
schema_tru_att = f"{trusted_zone}/{maxpar}/{atendimento}/"
schema_tru_master = f"{trusted_zone}/{master}/"

# REPORT
rep_wrk_att_path = path_bucket + schema_rep_att_a24h + "REP_WORKER_ATENDIMENTO_ASSISTENCIA_GENESIS"

# TRUSTED
tru_rec_path = path_bucket + schema_tru_att + "TRU_RECLAMACAO_ATENDIMENTO"
tru_data_path = path_bucket + schema_tru_master + "TRU_DOM_DATA"

# --- TABELA DE DESTINO (PATH) --- #
path_table = path_bucket + schema_ref_a24h + nome_tabela

# --- VALIDAÇÃO DE INGESTÃO --- #
lista_path_dependencias = [
    rep_wrk_att_path,
    tru_rec_path,
    tru_data_path
]

if "{{ambiente}}" == "prod-us-1":
    decisor.decisor_de_incidentes(lista_path_dependencias)
    valida_atualizacao_dependencias(rep_wrk_att_path)
    # valida_atualizacao_dependencias(tru_rec_path)
    valida_atualizacao_dependencias(tru_data_path, valida_ingestao=False)

# --- LEITURA E FILTROS DAS TABELAS --- #
cols_genesis = [
    "COD_PROTOCOLO_ATENDIMENTO",
    "DAT_CRIACAO_ATENDIMENTO",
    "TPO_ASSISTENCIA",
    "IDT_STATUS_ATENDIMENTO",
    "TPO_ATENDIMENTO",
    "IDT_GEROU_ACIONAMENTO",
    "COD_CLIENTE",
    "NOM_CLIENTE",
]

data_referencia = datetime.strptime("2026-01-01", "%Y-%m-%d")
pattern = r"([A-Z]\d{11}(?:/\d+)?|\d{14}(?:/\d+)?)"

de_para_seguradora = {
    'GENESIS': {
        '2': 'ALLSEG',
        '3': 'SERASA',
        '5': 'FAIRFAX',
        '6': 'SANCOR',
        '7': 'ITURAN',
        '8': 'REDE TOTAL BENEFICIOS',
        '9': 'BANESTES',
        '11': 'TOO SEGUROS',
        '12': 'OMNI',
        '13': 'SANTANDER',
        '14': 'DARWIN',
        '15': 'CAR SYSTEM',
        '16': 'YOUSE',
        '17': 'SAFRA',
        '19': 'ALFA',
        '20': 'NEO',
        '21': 'ASSURANT',
        '22': 'PLUXEE',
        '24': 'AKAD',
        '27': 'MBM',
        '28': 'AIG',
        '29': 'AVLA',
        '30': 'PORTO SEGURO',
        '31': 'IDEAL',
        '32': 'PREVISUL',
        '33': 'GRINGO',
        '34': 'ZURICH',
        '35': 'PASI',
        '36': 'VR BENEFICIOS',
        '39': 'CHUBB',
        '41': 'POC',
        '43': 'PST',
        '45': 'IZA',
        '46': 'ARCA',
        '47': 'BP',
        '49': 'KAKAU',
        '50': 'ZUL+',
        '53': 'VD',
        '54': 'TEM ADMINISTRADORA DE CARTOES',
        '55': 'COBUCCIO',
        '56': 'VIVA MAIS PLAN',
        '57': 'BIZ',
        '59': 'ZURICH AUTO',
        '60': 'HDI COLOMBIA',
        '61': 'KOVR',
        '62': 'AXA',
    },
    'RECLAMACAO': {
        '176': 'SANCOR',
        '177': 'ZURICH',
        '178': 'CHUBB',
        '180': 'YOUSE',
        '181': 'PREVISUL',
        '182': 'PLUXEE',
        '183': 'EZZE',
        '184': 'AKAD',
        '185': 'PORTO SEGURO',
        '186': 'SAFRA',
        '187': 'ITAU',
        '193': 'ALFA',
        '196': 'CAR SYSTEM',
        '199': 'DARWIN',
        '202': 'MBM',
        '203': 'ASSURANT',
        '204': 'SANTANDER',
        '205': 'CNP',
        '206': 'PASI',
        '207': 'AIG',
        '208': 'AVLA',
        '211': 'FAIRFAX',
        '221': 'ITURAN',
        '223': 'GRINGO',
        '224': 'AXA',
        '234': 'TOO SEGUROS',
        '235': 'NEO',
        '236': 'REDE TOTAL BENEFICIOS',
        '237': 'BANESTES',
        '240': 'SERASA',
        '242': 'ALLSEG',
        '246': 'POSITRON',
        '249': 'IZA',
        '250': 'BP',
        '253': 'GODRIVE',
        '255': 'BIZ',
        '257': 'KAKAU',
        '263': 'TEM ADMINISTRADORA DE CARTOES',
        '265': 'VIVA MAIS PLAN',
        '269': 'ZURICH AUTO',
        '270': 'KOVR',
        '271': 'OMNI',
    },
}

linhas_de_para_seguradora = [
    (origem, codigo, seguradora)
    for origem, seguradoras in de_para_seguradora.items()
    for codigo, seguradora in seguradoras.items()
]

mapa_seguradora = (
    spark.createDataFrame(
        linhas_de_para_seguradora,
        ["ORIGEM_MAPEAMENTO", "COD_ORIGEM", "DSC_SEGURADORA_PADRONIZADA"],
    )
    .withColumn("COD_ORIGEM", trim(col("COD_ORIGEM").cast("string")))
)

tru_rec = (
    spark.read.format("delta")
    .load(tru_rec_path)
    .filter((col("DSC_SEGURADORA").contains("IKE") | col("DSC_SEGURADORA").contains("A24H")))
    .withColumn("ORIGEM_MAPEAMENTO", lit("RECLAMACAO"))
)
log.info("TRU_RECLAMACAO_ATENDIMENTO carregada")

tru_data = (
    spark.read.format("delta")
    .load(tru_data_path)
    .select(to_date(col("DSC_DATA"), "dd/MM/yyyy").alias("DAT_REFERENCIA"))
    .filter(
        (col("DAT_REFERENCIA") >= lit("2026-01-01").cast("date"))
        & (col("DAT_REFERENCIA") <= current_date())
    )
)
log.info("TRU_DOM_DATA carregada")

att_gen = (
    spark.read.format("delta")
    .load(rep_wrk_att_path)
    .select(cols_genesis)
    .filter(
        (col("COD_PROTOCOLO_ATENDIMENTO") != "0")
    )
)
log.info("REP_WORKER_ATENDIMENTO_ASSISTENCIA_GENESIS carregada")

# --- UNIÃO DAS BASES DE RECLAMAÇÃO --- #
reclamacao_base = (
    tru_rec
    .withColumn(
        "COD_PROTOCOLO",
        when(
            (~trim(col("COD_PROTOCOLO").cast("string")).isin("", "0")),
            trim(col("COD_PROTOCOLO").cast("string")),
        ).otherwise(
            regexp_extract(col("DSC_ACAO_TOMADA").cast("string"), pattern, 1)
        ),
    )
    .withColumn(
        "COD_PROTOCOLO",
        when((trim(col("COD_PROTOCOLO")) == ""), lit("0")).otherwise(
            col("COD_PROTOCOLO")
        ),
    )
    .withColumn("DAT_RECLAMACAO", to_date(col("DAT_RECLAMACAO")))
)

cols_reclamacao = reclamacao_base.columns

reclamacao_enriquecida = (
    reclamacao_base
    .alias("REC")
    .join(
        broadcast(mapa_seguradora).alias("MAP"),
        (
            (col("REC.ORIGEM_MAPEAMENTO") == col("MAP.ORIGEM_MAPEAMENTO"))
            & (trim(col("REC.COD_SEGURADORA").cast("string")) == col("MAP.COD_ORIGEM"))
        ),
        "left",
    )
    .select(
        *[
            col(f"REC.{coluna}")
            for coluna in cols_reclamacao
            if coluna not in ("DSC_SEGURADORA", "COD_PROTOCOLO")
        ],
        coalesce(
            col("MAP.DSC_SEGURADORA_PADRONIZADA"),
            col("REC.DSC_SEGURADORA"),
        ).alias("DSC_SEGURADORA"),
        col("REC.COD_PROTOCOLO").alias("COD_PROTOCOLO"),
    )
)

# --- BASE DE ATENDIMENTO GENESIS --- #
atendimento_genesis = (
    att_gen.withColumn(
        "DAT_CRIACAO_ATENDIMENTO", to_date(col("DAT_CRIACAO_ATENDIMENTO"))
    )
    .select(
        "COD_PROTOCOLO_ATENDIMENTO",
        "DAT_CRIACAO_ATENDIMENTO",
        "TPO_ASSISTENCIA",
        "IDT_STATUS_ATENDIMENTO",
        "TPO_ATENDIMENTO",
        "IDT_GEROU_ACIONAMENTO",
        col("COD_CLIENTE").alias("COD_SEGURADORA"),
        col("NOM_CLIENTE").alias("DSC_SEGURADORA"),
    )
    .withColumn("ORIGEM_MAPEAMENTO", lit("GENESIS"))
    .alias("ATT")
    .join(
        broadcast(mapa_seguradora).alias("MAP"),
        (
            (col("ATT.ORIGEM_MAPEAMENTO") == col("MAP.ORIGEM_MAPEAMENTO"))
            & (trim(col("ATT.COD_SEGURADORA").cast("string")) == col("MAP.COD_ORIGEM"))
        ),
        "left",
    )
    .select(
        col("ATT.COD_PROTOCOLO_ATENDIMENTO").alias("COD_PROTOCOLO_ATENDIMENTO"),
        col("ATT.DAT_CRIACAO_ATENDIMENTO").alias("DAT_CRIACAO_ATENDIMENTO"),
        col("ATT.TPO_ASSISTENCIA").alias("TPO_ASSISTENCIA"),
        col("ATT.IDT_STATUS_ATENDIMENTO").alias("IDT_STATUS_ATENDIMENTO"),
        col("ATT.TPO_ATENDIMENTO").alias("TPO_ATENDIMENTO"),
        col("ATT.IDT_GEROU_ACIONAMENTO").alias("IDT_GEROU_ACIONAMENTO"),
        col("ATT.COD_SEGURADORA").alias("COD_SEGURADORA"),
        coalesce(
            col("MAP.DSC_SEGURADORA_PADRONIZADA"),
            col("ATT.DSC_SEGURADORA"),
        ).alias("DSC_SEGURADORA"),
    )
    .dropDuplicates(
        ["COD_PROTOCOLO_ATENDIMENTO", "DAT_CRIACAO_ATENDIMENTO"]
    )
)

ordem_evento_mes = Window.partitionBy(
    year(col("DAT_CRIACAO_ATENDIMENTO")),
    month(col("DAT_CRIACAO_ATENDIMENTO")),
    "COD_EVENTO",
).orderBy(
    col("COD_PROTOCOLO_ATENDIMENTO").asc(),
)

eventos_genesis = (
    atendimento_genesis
    .filter(col("TPO_ATENDIMENTO") != "DUVIDA")
    .withColumn(
        "COD_EVENTO",
        split(col("COD_PROTOCOLO_ATENDIMENTO"), "/")[0],
    )
    .withColumn("NUM_ORDEM_EVENTO_MES", row_number().over(ordem_evento_mes))
    .filter(col("NUM_ORDEM_EVENTO_MES") == 1)
    .groupBy(
        col("DAT_CRIACAO_ATENDIMENTO").alias("DAT_REFERENCIA"),
        "TPO_ASSISTENCIA",
        "IDT_STATUS_ATENDIMENTO",
        "DSC_SEGURADORA",
    )
    .agg(count("COD_EVENTO").alias("QTD_EVENTOS"))
)

# --- MANIFESTACOES: VINCULO POR PROTOCOLO SEM RESTRICAO DE DATA --- #
referencia_protocolos = atendimento_genesis.groupBy("COD_PROTOCOLO_ATENDIMENTO").agg(
    *[
        when(countDistinct(col(campo)) == 1, first(col(campo), ignorenulls=True))
        .alias(campo)
        for campo in ["TPO_ASSISTENCIA", "IDT_STATUS_ATENDIMENTO", "DSC_SEGURADORA"]
    ]
)

manifestacoes = (
    reclamacao_enriquecida.alias("REC")
    .join(
        referencia_protocolos.alias("ATT"),
        (col("REC.COD_PROTOCOLO") == col("ATT.COD_PROTOCOLO_ATENDIMENTO"))
        & (col("REC.COD_PROTOCOLO") != "0"),
        "left",
    )
    .select(
        col("REC.DAT_RECLAMACAO").alias("DAT_REFERENCIA"),
        col("REC.COD_MANIFESTACAO").alias("COD_MANIFESTACAO"),
        col("REC.DSC_ORIGEM").alias("DSC_ORIGEM_PORTAL"),
        col("REC.DSC_MOTIVO_VENUS").alias("DSC_MOTIVO_RECLAMACAO"),
        col("ATT.TPO_ASSISTENCIA").alias("TPO_ASSISTENCIA"),
        col("ATT.IDT_STATUS_ATENDIMENTO").alias("IDT_STATUS_ATENDIMENTO"),
        coalesce(col("REC.DSC_SEGURADORA"), col("ATT.DSC_SEGURADORA"))
        .alias("DSC_SEGURADORA"),
        col("REC.IDT_PROCEDENTE").alias("IDT_PROCEDENTE"),
    )
)

group_cols = [
    "DAT_REFERENCIA", "TPO_ASSISTENCIA",
    "IDT_STATUS_ATENDIMENTO", "DSC_SEGURADORA",
]

# Preserva as regras de elegibilidade do numerador.
tabela_manifestacao = manifestacoes.groupBy(*group_cols).agg(
    count(
        when(
            (
                col("DSC_ORIGEM_PORTAL").isin(
                    "RECLAMAÇÃO SEGURADORA",
                    "AÇÃO JUDICIAL",
                    "PROCON",
                )
            )
            & ~(
                col("DSC_MOTIVO_RECLAMACAO").contains("RECLAMAÇÃO EM DUPLICIDADE")
                | col("DSC_MOTIVO_RECLAMACAO").contains("RECLAMAÇÃO INDEVIDA")
            ),
            col("COD_MANIFESTACAO"),
        )
    ).alias("QTD_MANIFESTACAO"),
    count(
        when(
            (col("IDT_PROCEDENTE") == "SIM")
            & (
                col("DSC_ORIGEM_PORTAL").isin(
                    "RECLAMAÇÃO SEGURADORA",
                    "AÇÃO JUDICIAL",
                    "PROCON",
                )
            )
            & ~(
                col("DSC_MOTIVO_RECLAMACAO").contains("RECLAMAÇÃO EM DUPLICIDADE")
                | col("DSC_MOTIVO_RECLAMACAO").contains("RECLAMAÇÃO INDEVIDA")
            ),
            col("COD_MANIFESTACAO"),
        )
    ).alias("QTD_MANIFESTACAO_PROCEDENTE"),
)

# --- DENOMINADOR: TODOS OS ATENDIMENTOS, COM OU SEM RECLAMACAO --- #
tabela_atendimento = (
    atendimento_genesis
    .withColumn("DAT_REFERENCIA", col("DAT_CRIACAO_ATENDIMENTO"))
    .groupBy(*group_cols)
    .agg(
        countDistinct(
            when(col("IDT_GEROU_ACIONAMENTO") == "SIM", col("COD_PROTOCOLO_ATENDIMENTO"))
        ).alias("QTD_ACIONAMENTO"),
        countDistinct(
            when(col("TPO_ATENDIMENTO") != "DUVIDA", col("COD_PROTOCOLO_ATENDIMENTO"))
        ).alias("QTD_SERVICOS"),
    )
)

# Cada bloco contribui somente com suas metricas e preserva a data do fato.
metricas = [
    "QTD_MANIFESTACAO", "QTD_MANIFESTACAO_PROCEDENTE",
    "QTD_ACIONAMENTO", "QTD_SERVICOS", "QTD_EVENTOS",
]


def completa_metricas(base):
    return base.select(
        *group_cols,
        *[
            col(metrica) if metrica in base.columns else lit(0).alias(metrica)
            for metrica in metricas
        ],
    )


fatos_idsac = (
    completa_metricas(tabela_manifestacao)
    .unionByName(completa_metricas(tabela_atendimento))
    .unionByName(completa_metricas(eventos_genesis))
)

base_idsac = tru_data.join(fatos_idsac, "DAT_REFERENCIA", "left")
tabela = base_idsac.groupBy(*group_cols).agg(
    *[sum(col(metrica)).alias(metrica) for metrica in metricas]
)

# --- CAST E PADRONIZAÇÃO DE NULOS --- #
col_cast_tabela = {
    "DAT_REFERENCIA": ["date", "1800-01-01"],
    "TPO_ASSISTENCIA": ["string", "N/D"],
    "IDT_STATUS_ATENDIMENTO": ["string", "N/D"],
    "DSC_SEGURADORA": ["string", "N/D"],
    "QTD_MANIFESTACAO": ["int", 0],
    "QTD_MANIFESTACAO_PROCEDENTE": ["int", 0],
    "QTD_ACIONAMENTO": ["int", 0],
    "QTD_SERVICOS": ["int", 0],
    "QTD_EVENTOS": ["int", 0],
}

exprs = []
for coluna in tabela.columns:
    dtype = col_cast_tabela[coluna][0]
    default_null = col_cast_tabela[coluna][1]
    coluna_expr = col(coluna)
    is_nulo_ou_vazio = coluna_expr.isNull() | (trim(coluna_expr.cast("string")) == "")
    if dtype == "timestamp":
        expr = (
            when(is_nulo_ou_vazio, lit(default_null).cast("timestamp"))
            .otherwise(coluna_expr.cast("timestamp"))
            .alias(coluna)
        )
    elif dtype == "date":
        expr = (
            when(is_nulo_ou_vazio, lit(default_null).cast("date"))
            .otherwise(coluna_expr.cast("date"))
            .alias(coluna)
        )
    elif dtype is not None:
        expr = (
            when(is_nulo_ou_vazio, lit(default_null))
            .otherwise(coluna_expr)
            .cast(dtype)
            .alias(coluna)
        )
    else:
        expr = coluna_expr.alias(coluna)
    exprs.append(expr)

tabela = (
    tabela.select(*exprs)
    .select(
        "DAT_REFERENCIA",
        "TPO_ASSISTENCIA",
        "IDT_STATUS_ATENDIMENTO",
        "DSC_SEGURADORA",
        "QTD_MANIFESTACAO",
        "QTD_MANIFESTACAO_PROCEDENTE",
        "QTD_ACIONAMENTO",
        "QTD_SERVICOS",
        "QTD_EVENTOS",
    )
)
log.info("INÍCIO ESCRITA DA TABELA NO DATA LAKEHOUSE")

# --- ESCRITA DA TABELA --- #
log.info("INÍCIO ESCRITA DA REFINED NO LAKE")

tabela.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_table)

log.info("ESCRITA DA REFINED NO LAKE EXECUTADA COM SUCESSO")

delta_table = DeltaTable.forPath(spark, path_table)
delta_table.vacuum(72)
delta_table.generate("symlink_format_manifest")
log.info("Manifesto criado com sucesso!")
 