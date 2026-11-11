# --- BIBLIOTECAS E DEPENDÊNCIAS --- #
from pyspark.sql.functions import *
from utils.zoro import *
from datetime import datetime, timezone, timedelta
import logging
from utils_maxpar.funcoes import valida_atualizacao_dependencias
from utils_maxpar import classe_decisor_incidentes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# --- VARIÁVEIS / PARAMETRIZAÇÃO --- #
eng = "GABRIEL_GON"
path_s3 = "s3://"
bucket = "autoglass-lakehouse-dev-us-1"
nome_tabela = "REF_RECLAMACAO_MAXPAR"

# --- INICIALIZAÇÃO SPARKSESSION (DELTA) --- #
configs = [("spark.databricks.delta.optimizeWrite.enabled", "true"),
    ("spark.databricks.delta.autoCompact.enabled", "true"),
    ("spark.databricks.delta.retentionDurationCheck.enabled", "false")]

dl = DeltaLake(app_name = nome_tabela, configs = configs)
spark = dl.build()
decisor = classe_decisor_incidentes.DecisorIncidentes(spark)

trusted_zone = "TRUSTED"
report_zone = "REPORT"
refined_zone = "REFINED"
master = "MASTER"
maxpar = "MAXPAR"
apolice = "APOLICE"
atendimento = "ATENDIMENTO"
a24H = "A24H"
cliente = "CLIENTE"

# PATHs
path_bucket = f"{path_s3}{bucket}/"
schema_ref_att = f"{refined_zone}/{maxpar}/{atendimento}/"
schema_rep_att = f"{report_zone}/{maxpar}/{atendimento}/"
schema_tru_att = f"{trusted_zone}/{maxpar}/{atendimento}/"
schema_tru_a24 = f"{trusted_zone}/{maxpar}/{a24H}/"
schema_tru_cliente = f"{trusted_zone}/{maxpar}/{cliente}/"
schema_tru_apolice = f"{trusted_zone}/{maxpar}/{apolice}/"
schema_tru_master = f"{trusted_zone}/{master}/"

# --- TABELA DE DESTINO (PATH) --- #
path_table = path_bucket + schema_ref_att + nome_tabela

# TRUSTED
tru_ven_att = path_bucket + schema_tru_a24 + "TRU_ATENDIMENTO_VENITI"
tru_gen_att = path_bucket + schema_tru_a24 + "TRU_GENESIS_ATENDIMENTO"
tru_gen_aci = path_bucket + schema_tru_a24 + "TRU_GENESIS_ACIONAMENTO"
tru_gen_duv = path_bucket + schema_tru_a24 + "TRU_GENESIS_ATENDIMENTO_DUVIDA"
tru_rec_path = path_bucket + schema_tru_att + "TRU_RECLAMACAO_ATENDIMENTO_RESUMIDA"
tru_ilha_path = path_bucket + schema_tru_att + "TRU_APURACAO_ILHA_SINISTRO"
tru_seguradora_path = path_bucket + schema_tru_cliente + "TRU_SEGURADORA_MAXPAR"
tru_apolice_path = path_bucket + schema_tru_apolice + "TRU_APOLICE_ITEM"
tru_script_att_path = path_bucket + schema_tru_apolice + "TRU_SCRIPT_ATENDIMENTO_MAXPAR"
tru_usuario_generico_path = path_bucket + schema_tru_master + "TRU_USUARIO_GENERICO"
tru_empresa_path = path_bucket + schema_tru_master + "TRU_EMPRESA"

# --- VALIDAÇÃO DE INGESTÃO --- #

lista_path_dependencias = [tru_ven_att,
    tru_gen_att,
    tru_gen_aci,
    tru_gen_duv,
    tru_rec_path,
    tru_ilha_path,
    tru_seguradora_path,
    tru_apolice_path,
    tru_script_att_path,
    tru_usuario_generico_path,
    tru_empresa_path
]

if "{{ambiente}}" == "prod-us-1":
    decisor.decisor_de_incidentes(lista_path_dependencias)
    valida_atualizacao_dependencias(tru_ven_att, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_gen_att, valida_crescimento=False)
    valida_atualizacao_dependencias(tru_gen_aci, valida_crescimento=False)
    valida_atualizacao_dependencias(tru_gen_duv, valida_crescimento=False)
    valida_atualizacao_dependencias(tru_rec_path)
    valida_atualizacao_dependencias(tru_ilha_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_seguradora_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_apolice_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_script_att_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_usuario_generico_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_empresa_path, valida_ingestao=False)

# --- LEITURA E FILTROS DAS TABELAS --- #
cols_veniti = [
    "COD_PROTOCOLO_ATENDIMENTO",
    "COD_PLANO",
    "TPO_ASSISTENCIA",
    "TPO_STATUS_ATENDIMENTO",
    "DSC_NOME_PRESTADOR",
    "DSC_PLANO",
    "DSC_TIPO_SERVICO"
]

cols_genesis_att = [
    "COD_PROTOCOLO_ATENDIMENTO",
    "DSC_SEGMENTO",
    "TPO_SITUACAO_ATENDIMENTO",
    "DSC_CONTRATO",
    "COD_CONTRATO",
    "DSC_COBERTURA",
    "DSC_RELATO_EVENTO",
    "COD_CLIENTE",
    "NOM_USUARIO_ABERTURA",
]

cols_genesis_aci = [
    "COD_PROTOCOLO_ATENDIMENTO",
    "DSC_PRESTADOR_ACIONAMENTO"
]

cols_genesis_duvida = [
    "COD_PROTOCOLO_ATENDIMENTO",
    "TPO_STATUS_ATENDIMENTO",
    "DSC_COBERTURA",
]

cols_seguradora = [
    "COD_SEGURADORA",
    "DSC_GRUPO_SEGURADORA",
]

cols_apolice = [
    "COD_SEGURADORA",
    "NUM_APOLICE",
    "NUM_CORRETOR",
    "COD_CORRETOR",
    "DSC_NOME_CORRETORA",
]

cols_script = [
    "COD_GRUPO_TIPO_SCRIPT",
    "COD_SCRIPT",
    "COD_TIPO_SCRIPT",
    "DSC_GRUPO_TIPO_SCRIPT",
]

cols_empresa = [
    "COD_CLIENTE",
    "NUM_SEQUENCIA",
]

cols_ilha = [
    "COD_USUARIO",
    "DSC_ILHA",
    "DAT_CADASTRO_ANALISTA",
]

cols_usuario_generico = [
    "COD_USUARIO",
    "NOM_LOGIN",
]

data_referencia = datetime.strptime("2026-01-01", "%Y-%m-%d")
pattern = r"([A-Z]\d{11}(?:/\d+)?|\d{14}(?:/\d+)?)"

tru_rec = (
    spark.read.format("delta")
    .load(tru_rec_path)
)
log.info("TRU_RECLAMACAO_ATENDIMENTO carregada")

tru_ilha = (
    spark.read.format("delta")
    .load(tru_ilha_path)
    .select(cols_ilha)
).alias("ILHA")
log.info("TRU_APURACAO_ILHA_SINISTRO carregada")

tru_seguradora = (
    spark.read.format("delta")
    .load(tru_seguradora_path)
    .select(cols_seguradora)
    .groupBy("COD_SEGURADORA")
    .agg(min("DSC_GRUPO_SEGURADORA").alias("DSC_GRUPO_SEGURADORA"))
).alias("SEG")
log.info("TRU_SEGURADORA carregada")

tru_apolice = (
    spark.read.format("delta")
    .load(tru_apolice_path)
    .select(cols_apolice)
)
log.info("TRU_APOLICE_ITEM carregada")

tru_script = (
    spark.read.format("delta")
    .load(tru_script_att_path)
    .select(cols_script)
    .groupBy("COD_GRUPO_TIPO_SCRIPT", "COD_SCRIPT", "COD_TIPO_SCRIPT")
    .agg(min("DSC_GRUPO_TIPO_SCRIPT").alias("DSC_GRUPO_TIPO_SCRIPT"))
).alias("SCR")
log.info("TRU_SCRIPT_ATENDIMENTO carregada")

tru_empresa = (
    spark.read.format("delta")
    .load(tru_empresa_path)
    .select(cols_empresa)
    .groupBy("COD_CLIENTE")
    .agg(min("NUM_SEQUENCIA").alias("NUM_SEQUENCIA"))
).alias("EMP")
log.info("TRU_EMPRESA carregada")

tru_usuario_generico = (
    spark.read.format("delta")
    .load(tru_usuario_generico_path)
    .select(cols_usuario_generico)
    .groupBy("COD_USUARIO")
    .agg(min("NOM_LOGIN").alias("NOM_LOGIN"))
).alias("USR")
log.info("TRU_USUARIO_GENERICO carregada")

# --- BASEs DE RECLAMAÇÃO --- #
reclamacao = (
    tru_rec
    .withColumn(
        "TPO_EMPRESA",
        when(col("DSC_SEGURADORA").contains("IKE") | col("DSC_SEGURADORA").contains("A24H"), lit("MAXPAR - A24H"))
        .otherwise(lit("MAXPAR - VIDROS")),
    )
    .withColumn(
        "COD_PROTOCOLO",
        when(
            col("TPO_EMPRESA") == "MAXPAR - A24H",
            regexp_extract(col("DSC_ACAO_TOMADA").cast("string"), pattern, 1),
        ).otherwise(lit("0")),
    )
    .withColumn(
        "COD_PROTOCOLO",
        when(trim(col("COD_PROTOCOLO")) == "", lit(None)).otherwise(col("COD_PROTOCOLO")),
    )
    .withColumn(
        "IDT_DATA_AGENDA",
        when(
            col("TPO_EMPRESA") == "MAXPAR - A24H",
            when(
                col("DSC_OBSERVACAO").isNull() | (col("DSC_OBSERVACAO") == "") | (col("DSC_OBSERVACAO") == "N/D"),
                lit("SIM"),
            ).otherwise(lit("NAO")),
        ).otherwise(lit("N/D")),
    )
    .withColumn(
        "DSC_MOTIVO_RETORNO",
        when(col("DSC_MOTIVO_VENUS").like("%DUPLICIDADE%"), lit("DUPLICIDADE"))
        .when(col("DSC_MOTIVO_VENUS").like("%ATUALIZAÇÃO%"), lit("ATUALIZACAO"))
        .when(col("DSC_MOTIVO_VENUS").like("%QUESTIONAMENTO%"), lit("QUESTIONAMENTO"))
        .otherwise(lit("SEM RETORNO")),
    )
    .withColumn(
        "DSC_ORIGEM_RECLAMACAO",
        when(
            col("DSC_ORIGEM").isin(
                "AÇÃO JUDICIAL",
                "PROCON",
                "RECLAMAÇÃO FACEBOOK",
                "RECLAMAÇÃO NPS",
                "RECLAMAÇÃO GOOGLE",
                "RECLAMAÇÃO OUVIDORIA",
                "RECLAMAÇÃO RECLAME AQUI",
                "RECLAMAÇÃO RELACIONAMENTO COM CLIENTE",
                "RECLAMAÇÃO SEGURADORA",
            ),
            lit("CONSIDERAR"),
        )
        .otherwise(lit("DESCONSIDERAR")),
    )
).alias("REC")

protocolos_a24h = (
    reclamacao
    .filter(
        (col("TPO_EMPRESA") == "MAXPAR - A24H")
        & col("COD_PROTOCOLO").isNotNull()
        & (trim(col("COD_PROTOCOLO")) != "")
        & (col("COD_PROTOCOLO") != "0")
    )
    .select(col("COD_PROTOCOLO").alias("COD_PROTOCOLO_ATENDIMENTO"))
    .distinct()
)
log.info("PROTOCOLOS A24H PARA FILTRO DAS TRUSTEDS CARREGADOS")

tru_atendimento_veniti = (
    spark.read.format("delta")
    .load(tru_ven_att)
    .select(cols_veniti)
    .join((protocolos_a24h), on="COD_PROTOCOLO_ATENDIMENTO", how="left_semi")
)
log.info("TRU_ATENDIMENTO_VENITI CARREGADA COM FILTRO DE PROTOCOLOS A24H")

tru_genesis_atendimento = (
    spark.read.format("delta")
    .load(tru_gen_att)
    .select(cols_genesis_att)
    .join((protocolos_a24h), on="COD_PROTOCOLO_ATENDIMENTO", how="left_semi")
)
log.info("TRU_GENESIS_ATENDIMENTO CARREGADA COM FILTRO DE PROTOCOLOS A24H")

tru_genesis_acionamento = (
    spark.read.format("delta")
    .load(tru_gen_aci)
    .select(cols_genesis_aci)
    .join((protocolos_a24h), on="COD_PROTOCOLO_ATENDIMENTO", how="left_semi")
)
log.info("TRU_GENESIS_ACIONAMENTO CARREGADA COM FILTRO DE PROTOCOLOS A24H")

tru_genesis_atendimento_duvida = (
    spark.read.format("delta")
    .load(tru_gen_duv)
    .select(cols_genesis_duvida)
    .join((protocolos_a24h), on="COD_PROTOCOLO_ATENDIMENTO", how="left_semi")
)
log.info("TRU_GENESIS_ATENDIMENTO_DUVIDA CARREGADA COM FILTRO DE PROTOCOLOS A24H")

# --- ESTRUTURAÇÃO SCRIPT --- #
apolices = (
    tru_apolice
    .filter((col("COD_CORRETOR") != "0") | (col("NUM_CORRETOR") != "N/D"))
    .select(
        "COD_SEGURADORA",
        "NUM_APOLICE",
        "COD_CORRETOR",
        "NUM_CORRETOR",
        "DSC_NOME_CORRETORA",
    )
    .dropDuplicates(["COD_SEGURADORA", "NUM_APOLICE"])
    .alias("API")
)

ilha_mes = (
    tru_ilha.select(
        col("COD_USUARIO"),
        trunc(col("DAT_CADASTRO_ANALISTA"), "month").alias("DAT_MES_REFERENCIA"),
        col("DSC_ILHA"),
    )
    .dropDuplicates(["COD_USUARIO", "DAT_MES_REFERENCIA"])
    .alias("ILHA_MES")
)

genesis_filtrado = tru_genesis_atendimento.filter(
    (col("DSC_RELATO_EVENTO") != "ATENDIMENTO CRIADO NO VENITI")
    & (~col("COD_CLIENTE").isin("0", "4"))
    & (~col("NOM_USUARIO_ABERTURA").isin("NATALIA.LIMA", "PALOMA.SOUZA"))
)

plano_genesis = (
    genesis_filtrado
    .select(
        col("COD_CONTRATO"),
        upper(col("DSC_CONTRATO")).alias("DSC_CONTRATO"),
    )
    .where((col("DSC_CONTRATO") != "VAZIO") & (col("COD_CONTRATO") != "0"))
    .distinct()
)

plano_veniti = (
    tru_atendimento_veniti
    .select(
        col("COD_PLANO").alias("COD_CONTRATO"),
        upper(col("DSC_PLANO")).alias("DSC_CONTRATO"),
    )
    .where((col("DSC_CONTRATO") != "VAZIO") & (col("COD_CONTRATO") != "0"))
    .distinct()
)

apoio_plano = (
    plano_genesis
    .unionByName(plano_veniti)
    .groupBy("COD_CONTRATO")
    .agg(min("DSC_CONTRATO").alias("DSC_CONTRATO"))
    .alias("APP")
)

atendimento_veniti = (
    tru_atendimento_veniti
    .select(
        col("COD_PROTOCOLO_ATENDIMENTO").alias("COD_PROTOCOLO"),
        col("TPO_ASSISTENCIA"),
        col("TPO_STATUS_ATENDIMENTO").alias("TPO_STATUS_ATENDIMENTO"),
        col("DSC_NOME_PRESTADOR").alias("NOM_PRESTADOR"),
        col("DSC_PLANO").alias("NOM_PLANO"),
        col("DSC_TIPO_SERVICO").alias("NOM_TIPO_SERVICO"),
    )
    .distinct()
)

atendimento_genesis = (
    genesis_filtrado.alias("ATE")
    .join(
        tru_genesis_acionamento.alias("ACI"),
        on=col("ATE.COD_PROTOCOLO_ATENDIMENTO") == col("ACI.COD_PROTOCOLO_ATENDIMENTO"),
        how="left",
    )
    .join(
        apoio_plano,
        on=col("ATE.COD_CONTRATO") == col("APP.COD_CONTRATO"),
        how="left",
    )
    .select(
        col("ATE.COD_PROTOCOLO_ATENDIMENTO").alias("COD_PROTOCOLO"),
        when(col("ATE.DSC_SEGMENTO").isin("AUTO24H", "AUTO A24H"), lit("AUTO"))
        .when(col("ATE.DSC_SEGMENTO") == "PATRIMONIAL", lit("RESIDENCIAL"))
        .otherwise(col("ATE.DSC_SEGMENTO"))
        .alias("TPO_ASSISTENCIA"),
        col("ATE.TPO_SITUACAO_ATENDIMENTO").alias("TPO_STATUS_ATENDIMENTO"),
        col("ACI.DSC_PRESTADOR_ACIONAMENTO").alias("NOM_PRESTADOR"),
        upper(
            when(col("ATE.DSC_CONTRATO") == "VAZIO", col("APP.DSC_CONTRATO"))
            .otherwise(col("ATE.DSC_CONTRATO"))
        ).alias("NOM_PLANO"),
        col("ATE.DSC_COBERTURA").alias("NOM_TIPO_SERVICO"),
    )
    .distinct()
)

atendimento_duvida = (
    tru_genesis_atendimento_duvida
    .select(
        col("COD_PROTOCOLO_ATENDIMENTO").alias("COD_PROTOCOLO"),
        lit("DUVIDA").alias("TPO_ASSISTENCIA"),
        col("TPO_STATUS_ATENDIMENTO").alias("TPO_STATUS_ATENDIMENTO"),
        lit("VAZIO").alias("NOM_PRESTADOR"),
        lit("VAZIO").alias("NOM_PLANO"),
        col("DSC_COBERTURA").alias("NOM_TIPO_SERVICO"),
    )
    .distinct()
)

atendimento_assistencia = (
    atendimento_veniti
    .unionByName(atendimento_genesis)
    .unionByName(atendimento_duvida)
    .groupBy("COD_PROTOCOLO")
    .agg(
        min(
            struct(
                col("TPO_ASSISTENCIA"),
                col("TPO_STATUS_ATENDIMENTO"),
                col("NOM_PRESTADOR"),
                col("NOM_PLANO"),
                col("NOM_TIPO_SERVICO"),
            )
        ).alias("ATENDIMENTO")
    )
    .select(
        col("COD_PROTOCOLO"),
        col("ATENDIMENTO.TPO_ASSISTENCIA").alias("TPO_ASSISTENCIA"),
        col("ATENDIMENTO.TPO_STATUS_ATENDIMENTO").alias("TPO_STATUS_ATENDIMENTO"),
        col("ATENDIMENTO.NOM_PRESTADOR").alias("NOM_PRESTADOR"),
        col("ATENDIMENTO.NOM_PLANO").alias("NOM_PLANO"),
        col("ATENDIMENTO.NOM_TIPO_SERVICO").alias("NOM_TIPO_SERVICO"),
    )
    .alias("ASI")
)

# --- REFINADA --- #
rec_a24h = reclamacao.filter(col("TPO_EMPRESA") == "MAXPAR - A24H").alias("REC")
rec_vidros = reclamacao.filter(col("TPO_EMPRESA") == "MAXPAR - VIDROS").alias("REC")

def monta_refinada(base_reclamacao, incluir_atendimento=False):
    base_join = (
        base_reclamacao.join(
        broadcast(tru_script),
        on=(
            (col("REC.COD_GRUPO_TIPO_SCRIPT") == col("SCR.COD_GRUPO_TIPO_SCRIPT"))
            & (col("REC.COD_SCRIPT") == col("SCR.COD_SCRIPT"))
            & (col("REC.COD_TIPO_SCRIPT") == col("SCR.COD_TIPO_SCRIPT"))
        ),
        how="left",
    )
    .join(
        broadcast(tru_seguradora),
        on=col("REC.COD_SEGURADORA") == col("SEG.COD_SEGURADORA"),
        how="left",
    )
    .join(
        broadcast(tru_empresa),
        on=col("REC.COD_CREDENCIADO") == col("EMP.COD_CLIENTE"),
        how="left",
    )
    .join(
        apolices,
        on=(
            (col("REC.NUM_APOLICE") == col("API.NUM_APOLICE"))
            & (col("REC.COD_SEGURADORA") == col("API.COD_SEGURADORA"))
        ),
        how="left",
    )
    .join(
        broadcast(tru_usuario_generico),
        on=col("REC.COD_USUARIO_CRIACAO") == col("USR.COD_USUARIO"),
        how="left",
    )
    .join(
        ilha_mes,
        on=(
            (col("REC.COD_RESPONSAVEL") == col("ILHA_MES.COD_USUARIO"))
            & (
                trunc(col("REC.DAT_RECLAMACAO"), "month")
                == col("ILHA_MES.DAT_MES_REFERENCIA")
            )
        ),
        how="left",
    )
    )

    if incluir_atendimento:
        base_join = base_join.join(
            atendimento_assistencia,
            on=col("REC.COD_PROTOCOLO") == col("ASI.COD_PROTOCOLO"),
            how="left",
        )
        tpo_assistencia = col("ASI.TPO_ASSISTENCIA")
        tpo_status_atendimento = col("ASI.TPO_STATUS_ATENDIMENTO")
        nom_prestador = col("ASI.NOM_PRESTADOR")
        nom_plano = col("ASI.NOM_PLANO")
        nom_tipo_servico = col("ASI.NOM_TIPO_SERVICO")
    else:
        tpo_assistencia = lit("N/D")
        tpo_status_atendimento = lit("N/D")
        nom_prestador = lit("N/D")
        nom_plano = lit("N/D")
        nom_tipo_servico = lit("N/D")

    return (
        base_join
    .select(
        col("REC.COD_ATENDIMENTO").alias("COD_ATENDIMENTO"),
        col("REC.COD_ORDEM_SERVICO").alias("COD_ORDEM_SERVICO"),
        col("REC.COD_PROTOCOLO").alias("COD_PROTOCOLO"),
        tpo_assistencia.alias("TPO_ASSISTENCIA"),
        tpo_status_atendimento.alias("TPO_STATUS_ATENDIMENTO"),
        nom_prestador.alias("NOM_PRESTADOR"),
        nom_plano.alias("NOM_PLANO"),
        nom_tipo_servico.alias("NOM_TIPO_SERVICO"),
        col("REC.COD_MANIFESTACAO").alias("COD_MANIFESTACAO"),
        col("REC.TPO_EMPRESA").alias("TPO_EMPRESA"),
        to_date(col("REC.DAT_RECLAMACAO")).alias("DAT_RECLAMACAO"),
        to_date(col("REC.DAT_INICIO_ANALISE")).alias("DAT_ANALISE"),
        to_date(col("REC.DAT_CONCLUSAO")).alias("DAT_CONCLUSAO"),
        col("REC.COD_SEGURADORA").alias("COD_SEGURADORA"),
        col("REC.DSC_SEGURADORA").alias("DSC_SEGURADORA"),
        col("SEG.DSC_GRUPO_SEGURADORA").alias("DSC_GRUPO_SEGURADORA"),
        col("REC.NUM_APOLICE").alias("NUM_APOLICE"),
        col("REC.COD_CORRETOR").alias("COD_CORRETOR"),
        col("REC.NUM_CORRETOR").alias("NUM_CORRETOR"),
        when(col("REC.COD_SEGURADORA") == "56", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "62", substring(col("REC.COD_CORRETOR"), 6, 10))
        .when(col("REC.COD_SEGURADORA") == "65", concat(lit("20"), col("REC.COD_CORRETOR")))
        .when(col("REC.COD_SEGURADORA") == "23", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "43", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "52", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "7", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "55", col("API.DSC_NOME_CORRETORA"))
        .when(col("REC.COD_SEGURADORA") == "49", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "74", col("REC.NUM_CORRETOR"))
        .when(col("REC.COD_SEGURADORA") == "115", substring(col("REC.COD_CORRETOR"), 6, 10))
        .otherwise(lit("N/D"))
        .alias("DSC_SUSEP_CORRETOR"),
        col("REC.DSC_ORIGEM").alias("DSC_ORIGEM_PORTAL"),
        col("REC.DSC_MOTIVO_VENUS").alias("DSC_MOTIVO_RECLAMACAO"),
        col("REC.TPO_PENDENCIA_STATUS").alias("TPO_PENDENCIA_STATUS"),
        col("REC.TPO_PROCEDENTE"),
        col("REC.IDT_PROCEDENTE"),
        col("REC.DSC_MOTIVO_RETORNO").alias("DSC_MOTIVO_RETORNO"),
        col("REC.DSC_ORIGEM_RECLAMACAO").alias("DSC_ORIGEM_RECLAMACAO"),
        col("REC.IDT_DATA_AGENDA").alias("IDT_DATA_AGENDA"),
        col("REC.NUM_TEMPO_ANALISE").alias("NUM_TEMPO_ANALISE"),
        col("REC.IDT_ATRASO").alias("IDT_ATRASO"),
        col("REC.IDT_PRIORIDADE").alias("IDT_PRIORIDADE"),
        col("REC.COD_USUARIO_CRIACAO").alias("COD_USUARIO_CRIACAO"),
        col("USR.NOM_LOGIN").alias("NOM_LOGIN"),
        col("REC.NOM_USUARIO_CRIACAO").alias("NOM_USUARIO_RECLAMACAO"),
        col("REC.NOM_USUARIO_CONCLUSAO").alias("NOM_USUARIO_CONCLUSAO"),
        col("REC.NOM_USUARIO_INICIO_ANALISE").alias("NOM_USUARIO_ANALISE"),
        col("REC.COD_RESPONSAVEL").alias("COD_RESPONSAVEL_PROVIDENCIA"),
        col("REC.NOM_RESPONSAVEL").alias("NOM_RESPONSAVEL_PROVIDENCIA"),
        upper(col("REC.DSC_FUNCAO_RESPONSAVEL")).alias("DSC_FUNCAO_PROVIDENCIA"),
        col("REC.NOM_LOJA_RESPONSAVEL").alias("DSC_LOJA_RESPONSAVEL"),
        col("ILHA_MES.DSC_ILHA").alias("DSC_ILHA"),
        col("REC.COD_CREDENCIADO").alias("COD_CREDENCIADO_ATENDIMENTO"),
        col("REC.NOM_CLIENTE").alias("DSC_LOJA_ATENDIMENTO"),
        col("REC.DSC_CIDADE").alias("DSC_CIDADE"),
        col("REC.COD_UF").alias("COD_UF"),
        when(col("EMP.NUM_SEQUENCIA") == 999, lit("LOJA PRÓPRIA"))
        .when(
            (col("EMP.NUM_SEQUENCIA") != 999) & (col("REC.NOM_CLIENTE") == "N/D"),
            lit("SEM LOJA VINCULADA"),
        )
        .when(col("REC.COD_CREDENCIADO") == "3226932", lit("PROCESSO REEMBOLSO"))
        .otherwise(lit("CREDENCIADO"))
        .alias("TPO_LOJA"),
        col("REC.DSC_SCRIPT").alias("DSC_SCRIPT"),
        col("SCR.DSC_GRUPO_TIPO_SCRIPT").alias("DSC_GRUPO_TIPO_SCRIPT"),
        col("REC.TPO_PROVIDENCIA").alias("TPO_PROVIDENCIA"),
        col("REC.DSC_SUB_MOTIVO").alias("DSC_SUB_MOTIVO"),
        col("REC.DSC_AREA_OFENSORA").alias("DSC_AREA_OFENSORA"),
    )
)

refined = (
    monta_refinada(rec_a24h, incluir_atendimento=True)
    .unionByName(monta_refinada(rec_vidros))
)

# --- CAST E PADRONIZAÇÃO DE NULOS --- #
col_cast_refined = {c: [None, None] for c in refined.columns}
col_cast_refined.update(
    {
        "COD_ATENDIMENTO": ["string", "0"],
        "COD_CORRETOR": ["string", "0"],
        "COD_CREDENCIADO_ATENDIMENTO": ["string", "0"],
        "COD_MANIFESTACAO": ["string", "0"],
        "COD_ORDEM_SERVICO": ["string", "0"],
        "COD_PROTOCOLO": ["string", "0"],
        "COD_RESPONSAVEL_PROVIDENCIA": ["string", "0"],
        "COD_SEGURADORA": ["string", "0"],
        "COD_UF": ["string", "N/D"],
        "COD_USUARIO_CRIACAO": ["string", "0"],
        "DAT_ANALISE": ["date", "1800-01-01"],
        "DAT_CONCLUSAO": ["date", "1800-01-01"],
        "DAT_RECLAMACAO": ["date", "1800-01-01"],
        "DSC_AREA_OFENSORA": ["string", "N/D"],
        "DSC_CIDADE": ["string", "N/D"],
        "DSC_FUNCAO_PROVIDENCIA": ["string", "N/D"],
        "DSC_GRUPO_SEGURADORA": ["string", "N/D"],
        "DSC_GRUPO_TIPO_SCRIPT": ["string", "N/D"],
        "DSC_ILHA": ["string", "N/D"],
        "DSC_LOJA_ATENDIMENTO": ["string", "N/D"],
        "DSC_LOJA_RESPONSAVEL": ["string", "N/D"],
        "DSC_MOTIVO_RECLAMACAO": ["string", "N/D"],
        "DSC_MOTIVO_RETORNO": ["string", "N/D"],
        "IDT_DATA_AGENDA": ["string", "N/D"],
        "DSC_ORIGEM_PORTAL": ["string", "N/D"],
        "DSC_ORIGEM_RECLAMACAO": ["string", "N/D"],
        "DSC_SCRIPT": ["string", "N/D"],
        "DSC_SEGURADORA": ["string", "N/D"],
        "DSC_SUB_MOTIVO": ["string", "N/D"],
        "DSC_SUSEP_CORRETOR": ["string", "N/D"],
        "IDT_ATRASO": ["string", "N/D"],
        "IDT_PRIORIDADE": ["string", "N/D"],
        "IDT_PROCEDENTE": ["string", "N/D"],
        "NOM_LOGIN": ["string", "N/D"],
        "NOM_PLANO": ["string", "N/D"],
        "NOM_PRESTADOR": ["string", "N/D"],
        "NOM_RESPONSAVEL_PROVIDENCIA": ["string", "N/D"],
        "NOM_TIPO_SERVICO": ["string", "N/D"],
        "NOM_USUARIO_ANALISE": ["string", "N/D"],
        "NOM_USUARIO_CONCLUSAO": ["string", "N/D"],
        "NOM_USUARIO_RECLAMACAO": ["string", "N/D"],
        "NUM_APOLICE": ["string", "0"],
        "NUM_CORRETOR": ["string", "0"],
        "NUM_TEMPO_ANALISE": ["string", "0"],
        "TPO_ASSISTENCIA": ["string", "N/D"],
        "TPO_EMPRESA": ["string", "N/D"],
        "TPO_LOJA": ["string", "N/D"],
        "TPO_PENDENCIA_STATUS": ["string", "N/D"],
        "TPO_PROCEDENTE": ["string", "N/D"],
        "TPO_PROVIDENCIA": ["string", "N/D"],
        "TPO_STATUS_ATENDIMENTO": ["string", "N/D"],
    }
)

exprs = []
for coluna in refined.columns:
    dtype = col_cast_refined[coluna][0]
    default_null = col_cast_refined[coluna][1]
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
refined = refined.select(*exprs)

refined.explain()

# --- WRITE TABLE --- #

log.info("INÍCIO ESCRITA DA TABELA NO DATA LAKEHOUSE")
refined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_table)
dl.print_log(msg="Full load Executado com Sucesso")

dl.manifesto(path_table)
dl.data_ingestao(path_table)
dl.limpa_parquet_full_load(path_table)