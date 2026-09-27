# ========== BIBLIOTECAS PYSPARK ========== #

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.zoro import *
from delta.tables import DeltaTable
from datetime import datetime
from utils_maxpar.funcoes import valida_atualizacao_dependencias
from utils_maxpar import classe_decisor_incidentes
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


# ========== VARIAVEIS ========== #

eng = 'WELLINGTON_MELO, LEONARDO.SCHREDER'
path_s3 = "s3://"
bucket = "{{repositorioS3}}-{{ambiente}}"

rep_table = "REP_APOLICE_ITEM_MAXPAR_SANDBOX"

# ========== SPARK SESSION E CONFIGURACOES ========== #

configs = [("spark.databricks.delta.retentionDurationCheck.enabled", "false")]
dl = DeltaLake(app_name = rep_table, configs = configs)
spark = dl.build()
decisor = classe_decisor_incidentes.DecisorIncidentes(spark)

# ========== REPORT ========== #

# APOLICE - SANDBOX
rep_full_path_apolice = f"{path_s3}{bucket}/SANDBOX/MAXPAR/APOLICE/"
path_table = rep_full_path_apolice + rep_table

# LOCALIZACAO - REPORT
report = "REPORT"
maxpar = "MAXPAR"
produto = "PRODUTO"
rep_dataset_path = f"{report}/{maxpar}/{produto}/"
rep_full_path_localizacao = f"{path_s3}{bucket}/{rep_dataset_path}"

rep_localizacao_table = "REP_LOCALIZACAO"
rep_localizacao_path = rep_full_path_localizacao + rep_localizacao_table


# ========== TRUSTED ========== #

trusted = "TRUSTED"
apolice = "APOLICE"

maxpar = "MAXPAR"

## APOLICE
tru_dataset_path = f"{trusted}/{maxpar}/{apolice}/"
tru_full_path_fat = f"{path_s3}{bucket}/{tru_dataset_path}"

tru_apolice_item_path                = tru_full_path_fat + "TRU_APOLICE_ITEM"

## CLIENTE
cliente = "CLIENTE"
tru_dataset_cli_path = f"{trusted}/{maxpar}/{cliente}/"
tru_full_path_cli = f"{path_s3}{bucket}/{tru_dataset_cli_path}"

tru_seguradora_maxpar_path           = tru_full_path_cli + "TRU_SEGURADORA_MAXPAR"

## MAPEAMENTO
mapeamento = "MAPEAMENTO"

tru_dataset_path = f"{trusted}/{maxpar}/{mapeamento}/"
tru_full_path_assist = f"{path_s3}{bucket}/{tru_dataset_path}"

tru_mapeamento_chassi_path          = tru_full_path_assist + "TRU_MAPEAMENTO_CHASSI"
tru_mapeamento_fipe_path            = tru_full_path_assist + "TRU_MAPEAMENTO_FIPE"
tru_tabela_fipe_historico_path      = tru_full_path_assist + "TRU_TABELA_FIPE_HISTORICO"

# ========== VALIDA INGESTÃO ========== #

lista_path_dependencias = [tru_apolice_item_path, tru_mapeamento_chassi_path, tru_mapeamento_fipe_path, 
                            tru_tabela_fipe_historico_path, tru_seguradora_maxpar_path, rep_localizacao_path]

if "{{ambiente}}" == "prod-us-1":
    decisor.decisor_de_incidentes(lista_path_dependencias)
    valida_atualizacao_dependencias(tru_apolice_item_path)
    valida_atualizacao_dependencias(tru_mapeamento_chassi_path)
    valida_atualizacao_dependencias(tru_mapeamento_fipe_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_tabela_fipe_historico_path, valida_ingestao=False)
    valida_atualizacao_dependencias(tru_seguradora_maxpar_path, valida_ingestao=False)
    valida_atualizacao_dependencias(rep_localizacao_path, valida_ingestao=False)


# ========== LEITURA TABELA ========== #

tru_apolice_item = (spark.read.format("delta").load(tru_apolice_item_path))
log.info(f"Path {tru_apolice_item_path} carregado!")
    
tru_seguradora_maxpar = spark.read.format("delta").load(tru_seguradora_maxpar_path)
log.info(f"Path {tru_seguradora_maxpar_path} carregado!")

tru_mapeamento_chassi = spark.read.format("delta").load(tru_mapeamento_chassi_path)
log.info(f"Path {tru_mapeamento_chassi_path} carregado!")

tru_mapeamento_fipe = spark.read.format("delta").load(tru_mapeamento_fipe_path)
log.info(f"Path {tru_mapeamento_fipe_path} carregado!")

tru_tabela_fipe_historico = spark.read.format("delta").load(tru_tabela_fipe_historico_path)
log.info(f"Path {tru_tabela_fipe_historico_path} carregado!")

rep_localizacao = spark.read.format("delta").load(rep_localizacao_path)
log.info(f"Path {rep_localizacao_path} carregado!")


# ========== ESTRUTURA DO SCRIPT ========== #

# RETORNAR O ANO MODELO CORRETAMENTE

## DEFINIR CARAC_ANO_CHASSI E ANO_CHASSI_CARAC
CARAC_ANO_CHASSI = ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "R", "S", "T", "V", "W", "X", "Y", "1", "2", "3", "4", "5", "6", "7", "8", "9"] * 2
ANO_CHASSI_CARAC = list(range(1980, 2040))

# AJUSTAR CARAC_ANO_CHASSI PARA TER O MESMO COMPRIMENTO DE ANO_CHASSI_CARAC
CARAC_ANO_CHASSI = CARAC_ANO_CHASSI[:len(ANO_CHASSI_CARAC)]

# CRIAR RDD COM OS DADOS
RDD_COMPLETO = spark.sparkContext.parallelize(zip(CARAC_ANO_CHASSI, ANO_CHASSI_CARAC))

# CRIAR DATAFRAME A PARTIR DO RDD
DF_COMPLETO = RDD_COMPLETO.toDF(["CARAC_ANO_CHASSI", "ANO_CHASSI_CARAC"])

# FILTRAR DATAFRAME
ANO_REFERENCIA_MAIOR = datetime.now().year + 1
ANO_REFERENCIA_MENOR = datetime.now().year - 28
DF_FILTRADO = DF_COMPLETO.filter(
   (col("ANO_CHASSI_CARAC") >= lit(ANO_REFERENCIA_MENOR)) &
   (col("ANO_CHASSI_CARAC") <= lit(ANO_REFERENCIA_MAIOR))
)

DF_FILTRADO.createOrReplaceTempView("DE_PARA_ANO")

log.info(f"DE_PARA_ANO CARREGADA")

# FUNCAO PARA VALIDAR AS UFs
def validar_uf(coluna):
    
    ufs_validas = [
        "AC","AL","AP","AM","BA","CE","DF","ES","GO",
        "MA","MT","MS","MG","PA","PB","PR","PE","PI",
        "RJ","RN","RS","RO","RR","SC","SP","SE","TO"
    ]
    
    return when(coluna.isin(ufs_validas), coluna).otherwise("N/D")

log.info(f"Função de limpeza UFs carregada")

# ESTRUTURA DADOS DE APOLICE

## FUNCOES / EXPRESSOES AUXILIARES DAS REGRAS DE NEGOCIO

doc_cpf_cnpj_normatizado_expr = when(
    col("API.DOC_CPF_NORMATIZADO") != "N/D", col("API.DOC_CPF_NORMATIZADO")
).otherwise(col("API.DOC_CNPJ_NORMATIZADO"))

seguradoras_num_corretor = ["56", "23", "43", "52", "7", "49", "74"]

dsc_susep_corretor_padronizado_expr = (
    when(col("API.COD_SEGURADORA") == "62", substring(col("API.COD_CORRETOR"), 6, 10))
    .when(col("API.COD_SEGURADORA") == "65", concat(lit("20"), col("API.COD_CORRETOR")))
    .when(col("API.COD_SEGURADORA") == "55", col("API.DSC_NOME_CORRETORA"))
    .when(col("API.COD_SEGURADORA") == "115", substring(col("API.COD_CORRETOR"), 6, 10))
    .when(col("API.COD_SEGURADORA").isin(seguradoras_num_corretor), col("API.NUM_CORRETOR"))
    .otherwise(lit("N/D"))
)

def normalizar_cep(coluna):
    cep_sem_hifen = regexp_replace(coluna, "-", "")
    return when(
        (length(cep_sem_hifen) == 8) & (coluna != "N/D"),
        cep_sem_hifen
    ).when(
        (length(cep_sem_hifen) == 7) & (coluna != "N/D"),
        concat(lit("0"), cep_sem_hifen)
    ).when(
        (length(cep_sem_hifen) == 5) & (coluna != "N/D"),
        lit("00000000")
    ).otherwise(lit(None).cast("string"))

def data_por_prioridade(colunas):
    """Retorna a primeira coluna (na ordem informada) cujo ano seja diferente de 1800.
    Caso nenhuma seja valida, mantem o valor da primeira coluna - mesmo comportamento do CASE/ELSE original."""
    expr = col(colunas[0])
    for coluna in reversed(colunas):
        expr = when(year(col(coluna)) != 1800, col(coluna)).otherwise(expr)
    return expr.cast("date")

colunas_inicio_original = [
    "API.DAT_INICIO_ORIGINAL", "API.DAT_INICIO_ORIGINAL_FAROL", "API.DAT_INICIO_ORIGINAL_SRA",
    "API.DAT_INICIO_ORIGINAL_PARACHOQUE", "API.DAT_INICIO_ORIGINAL_TETO_SOLAR", "API.DAT_INICIO_ORIGINAL_UNDERCAR",
    "API.DAT_INICIO_ORIGINAL_LATARIA", "API.DAT_INICIO_ORIGINAL_MAQUINA",
]

colunas_inicio_vigencia = [
    "API.DAT_INICIO_VIGENCIA", "API.DAT_INICIO_VIGENCIA_FAROL", "API.DAT_INICIO_VIGENCIA_SRA",
    "API.DAT_INICIO_VIGENCIA_PARACHOQUE", "API.DAT_INICIO_VIGENCIA_TETO_SOLAR", "API.DAT_INICIO_VIGENCIA_UNDERCAR",
    "API.DAT_INICIO_VIGENCIA_LATARIA", "API.DAT_INICIO_VIGENCIA_MAQUINA",
]

colunas_fim_vigencia = [
    "API.DAT_FIM_VIGENCIA", "API.DAT_FIM_VIGENCIA_FAROL", "API.DAT_FIM_VIGENCIA_SRA",
    "API.DAT_FIM_VIGENCIA_PARACHOQUE", "API.DAT_FIM_VIGENCIA_TETO_SOLAR", "API.DAT_FIM_VIGENCIA_UNDERCAR",
    "API.DAT_FIM_VIGENCIA_LATARIA", "API.DAT_FIM_VIGENCIA_MAQUINA",
]

# TRU_SEGURADORA_MAXPAR

dado_apolice = (
    tru_apolice_item.alias("API")
    .join(broadcast(tru_seguradora_maxpar.alias("SEG")), col("API.COD_SEGURADORA") == col("SEG.COD_SEGURADORA"), "inner")
    .select(
        col("API.COD_SEGURADORA"),
        col("SEG.DSC_SEGURADORA"),
        col("API.NUM_APOLICE"),
        col("API.COD_ITEM"),
        when(
            normalizar_cep(col("API.END_CEP_AUXILIAR")) != "00000000",
            normalizar_cep(col("API.END_CEP_AUXILIAR"))
        )
        .when(
            normalizar_cep(col("API.END_CEP")) != "00000000",
            normalizar_cep(col("API.END_CEP"))
        ).when(
            normalizar_cep(col("API.END_CEP_RISCO")) != "00000000",
            normalizar_cep(col("API.END_CEP_RISCO"))
        ).when(
            normalizar_cep(col("API.END_CEP_PERNOITE")) != "00000000",
            normalizar_cep(col("API.END_CEP_PERNOITE"))
        ).otherwise(lit(None).cast("string")).alias("CEP_APOLICE"),
        col("API.DOC_CNPJ").alias("DOC_CPF_CNPJ"),
        doc_cpf_cnpj_normatizado_expr.alias("DOC_CPF_CNPJ_NORMATIZADO"),
        col("API.COD_CHASSI").alias("NUM_CHASSI"),
        col("API.COD_FIPE").alias("COD_FIPE_SEGURADORA"),
        col("API.DSC_PLACA"),
        col("API.IDT_BLINDADO"),
        col("API.COD_ANEXO"),
        col("API.COD_EXTERNO"),
        col("API.IDT_FROTA"),
        col("API.IDT_APOLICE_FROTA"),
        col("API.IDT_LOGO_MARCA").alias("IDT_LOGOMARCA"),
        col("API.IDT_LIVRE_ESCOLHA"),
        col("API.NOM_SEGURADO"),
        col("API.COD_CATEGORIA"),
        col("API.COD_PRODUTO"),
        col("API.COD_PLANO"),
        dsc_susep_corretor_padronizado_expr.alias("DSC_SUSEP_CORRETOR_PADRONIZADO"),
        col("API.NUM_CORRETOR"),
        col("API.COD_CORRETOR"),
        col("API.DSC_NOME_CORRETORA"),
        data_por_prioridade(colunas_inicio_original).alias("DAT_INICIO_VIGENCIA_ORIGINAL"),
        data_por_prioridade(colunas_inicio_vigencia).alias("DAT_INICIO_VIGENCIA"),
        data_por_prioridade(colunas_fim_vigencia).alias("DAT_FIM_VIGENCIA"),
    )
)

log.info(f"DADO_APOLICE CARREGADA")

# UNIFICAR APOLICE COM LOCALIZACAO
# Observação: não há ganho em fazer left join com CEP nulo. Como a maior parte
# das linhas não possui CEP válido, o filtro prévio reduz muito o shuffle.

dado_apolice_com_cep = dado_apolice.filter(
    col("CEP_APOLICE").isNotNull() & (trim(col("CEP_APOLICE")) != "")
)

dado_apolice_sem_cep = dado_apolice.filter(
    col("CEP_APOLICE").isNull() | (trim(col("CEP_APOLICE")) == "")
)

apolice_localizacao_validos = (
    dado_apolice_com_cep.alias("APO")
    .join(rep_localizacao.alias("LOC"), col("APO.CEP_APOLICE") == col("LOC.END_CEP"), "left")
    .select(
        col("APO.COD_SEGURADORA"),
        col("APO.DSC_SEGURADORA"),
        col("APO.NUM_APOLICE"),
        col("APO.COD_ITEM"),
        col("LOC.END_CEP"),
        col("APO.DAT_INICIO_VIGENCIA_ORIGINAL"),
        col("APO.DAT_INICIO_VIGENCIA"),
        col("APO.DAT_FIM_VIGENCIA"),
        col("APO.COD_FIPE_SEGURADORA"),
        col("APO.DOC_CPF_CNPJ"),
        col("APO.DOC_CPF_CNPJ_NORMATIZADO"),
        col("APO.NOM_SEGURADO"),
        col("APO.COD_EXTERNO"),
        col("APO.IDT_FROTA"),
        col("APO.IDT_APOLICE_FROTA"),
        col("APO.IDT_LOGOMARCA"),
        col("APO.IDT_LIVRE_ESCOLHA"),
        col("APO.COD_CATEGORIA"),
        col("APO.COD_PRODUTO"),
        col("APO.COD_PLANO"),
        col("LOC.DSC_CIDADE"),
        col("LOC.DSC_ABREVIACAO_UF").alias("DSC_ABREVIACAO_UF_ORIGINAL"),
        col("APO.DSC_PLACA"),
        col("APO.IDT_BLINDADO"),
        col("APO.COD_ANEXO"),
        col("APO.NUM_CHASSI"),
        col("APO.DSC_SUSEP_CORRETOR_PADRONIZADO"),
        col("APO.NUM_CORRETOR"),
        col("APO.COD_CORRETOR"),
        col("APO.DSC_NOME_CORRETORA"),
    )
)

apolice_localizacao_sem_cep = (
    dado_apolice_sem_cep.select(
        col("COD_SEGURADORA"),
        col("DSC_SEGURADORA"),
        col("NUM_APOLICE"),
        col("COD_ITEM"),
        lit(None).cast("string").alias("END_CEP"),
        col("DAT_INICIO_VIGENCIA_ORIGINAL"),
        col("DAT_INICIO_VIGENCIA"),
        col("DAT_FIM_VIGENCIA"),
        col("COD_FIPE_SEGURADORA"),
        col("DOC_CPF_CNPJ"),
        col("DOC_CPF_CNPJ_NORMATIZADO"),
        col("NOM_SEGURADO"),
        col("COD_EXTERNO"),
        col("IDT_FROTA"),
        col("IDT_APOLICE_FROTA"),
        col("IDT_LOGOMARCA"),
        col("IDT_LIVRE_ESCOLHA"),
        col("COD_CATEGORIA"),
        col("COD_PRODUTO"),
        col("COD_PLANO"),
        lit(None).cast("string").alias("DSC_CIDADE"),
        lit(None).cast("string").alias("DSC_ABREVIACAO_UF_ORIGINAL"),
        col("DSC_PLACA"),
        col("IDT_BLINDADO"),
        col("COD_ANEXO"),
        col("NUM_CHASSI"),
        col("DSC_SUSEP_CORRETOR_PADRONIZADO"),
        col("NUM_CORRETOR"),
        col("COD_CORRETOR"),
        col("DSC_NOME_CORRETORA"),
    )
)

apolice_localizacao = apolice_localizacao_validos.unionByName(apolice_localizacao_sem_cep)

log.info(f"APOLICE_LOCALIZACAO CARREGADA")

# BASE_FIPE_COMBUSTIVEL

janela_combustivel = Window.partitionBy(
    "COD_TABELA_FIPE", "DSC_CODIGO", "NUM_ANO", "DAT_REFERENCIA"
).orderBy(
    when(col("TPO_COMBUSTIVEL") == "GASOLINA", 1)
    .when(col("TPO_COMBUSTIVEL") == "ÃLCOOL", 2)
    .otherwise(3)
)

base_fipe_combustivel = (
    tru_tabela_fipe_historico
    .withColumn("ROW_NUM_COMBUSTIVEL", row_number().over(janela_combustivel))
)

log.info(f"BASE_FIPE_COMBUSTIVEL CARREGADA")

# BASE FIPE (AJUSTE DE DATA)

janela_data = Window.partitionBy("COD_TABELA_FIPE", "DSC_CODIGO", "NUM_ANO").orderBy(col("DAT_REFERENCIA").desc())

base_fipe_ajuste_data = (
    base_fipe_combustivel
    .where(col("ROW_NUM_COMBUSTIVEL") == 1)
    .withColumn("ROW_NUM_DATA", row_number().over(janela_data))
)

log.info(f"BASE_FIPE_AJUSTE_DATA CARREGADA")

# MAPEAMENTO

mapeamento = (
    tru_mapeamento_chassi.alias("CHA")
    .join(tru_mapeamento_fipe.alias("FIP"), col("CHA.COD_MAPEAMENTO_FIPE") == col("FIP.COD_MAPEAMENTO_FIPE"), "left")
    .join(broadcast(DF_FILTRADO.alias("DAM")), substring(col("CHA.DSC_CHASSI"), 10, 1) == col("DAM.CARAC_ANO_CHASSI"), "left")
    .select(
        col("CHA.COD_MAPEAMENTO_FIPE"),
        col("CHA.DSC_CODIGO_FIPE"),
        col("CHA.DSC_CHASSI").alias("NUM_CHASSI"),
        col("CHA.DSC_COR_MATRIZ"),
        col("FIP.COD_TABELA_FIPE"),
        col("DAM.ANO_CHASSI_CARAC").alias("NUM_ANO_MODELO"),
        when(
            col("FIP.DSC_MODELO").isin('VEÃCULOS BLINDADOS', 'CHASSIS INVÃLIDOS'), col("CHA.DSC_VEICULO")
        ).otherwise(col("FIP.DSC_MODELO")).alias("DSC_MODELO"),
    )
)

log.info(f"MAPEAMENTO CARREGADA")

# BASE REPORT

rep = (
    apolice_localizacao.alias("APO")
    .join(mapeamento.alias("MAP"), col("APO.NUM_CHASSI") == col("MAP.NUM_CHASSI"), "left")
    .join(
        base_fipe_combustivel.alias("FIPO"),
        (col("MAP.COD_TABELA_FIPE") == col("FIPO.COD_TABELA_FIPE")) &
        (col("MAP.NUM_ANO_MODELO") == col("FIPO.NUM_ANO")) &
        (date_trunc("MONTH", col("APO.DAT_INICIO_VIGENCIA_ORIGINAL")) == date_trunc("MONTH", col("FIPO.DAT_REFERENCIA"))) &
        (col("FIPO.ROW_NUM_COMBUSTIVEL") == 1),
        "left"
    )
    .join(
        base_fipe_ajuste_data.alias("FIPA"),
        (col("MAP.COD_TABELA_FIPE") == col("FIPA.COD_TABELA_FIPE")) &
        (col("MAP.NUM_ANO_MODELO") == col("FIPA.NUM_ANO")) &
        (col("FIPA.ROW_NUM_DATA") == 1),
        "left"
    )
    .select(
        concat(col("APO.COD_SEGURADORA"), col("APO.NUM_APOLICE"), col("APO.COD_ITEM")).cast("decimal(38,0)").alias("COD_CHAVE"),
        "APO.*",
        col("MAP.DSC_MODELO").alias("DSC_MODELO_VEICULO"),
        col("MAP.DSC_COR_MATRIZ"),
        col("MAP.NUM_ANO_MODELO").cast("string").alias("NUM_ANO_MODELO"),
        when(length(col("FIPA.DSC_CODIGO")) == 8, col("FIPA.DSC_CODIGO"))
        .when(length(col("MAP.DSC_CODIGO_FIPE")) == 8, col("MAP.DSC_CODIGO_FIPE"))
        .otherwise(lit("N/D")).alias("DSC_CODIGO_FIPE"),
        when(
            length(coalesce(col("FIPA.DSC_CODIGO"), col("MAP.DSC_CODIGO_FIPE"))) == 8,
            when(substring(coalesce(col("FIPA.DSC_CODIGO"), col("MAP.DSC_CODIGO_FIPE")), 1, 1).isin("0", "3"), lit("PASSEIO"))
            .when(substring(coalesce(col("FIPA.DSC_CODIGO"), col("MAP.DSC_CODIGO_FIPE")), 1, 1).isin("7", "8"), lit("MOTO"))
            .when(substring(coalesce(col("FIPA.DSC_CODIGO"), col("MAP.DSC_CODIGO_FIPE")), 1, 1) == "5", lit("CARGA"))
            .otherwise(lit("N/D"))
        ).otherwise(lit("N/D")).alias("TPO_VEICULO"),
        col("FIPO.VAL_VEICULO").alias("VAL_VEICULO_INICIO"),
        col("FIPA.VAL_VEICULO").alias("VAL_VEICULO_ATUAL"),
    )
)
log.info(f"REP CARREGADA")

dl.print_log(msg = "REPORT CARREGADA")

rep = rep.withColumn(
    'DSC_ABREVIACAO_UF',
    validar_uf(col('DSC_ABREVIACAO_UF_ORIGINAL'))
).drop(
    'DSC_ABREVIACAO_UF_ORIGINAL'
)

# ========== TRATAMENTO DE NULO ========== #

str_cols = ["DSC_SEGURADORA", "NOM_SEGURADO", "IDT_FROTA", "IDT_LOGOMARCA", "IDT_LIVRE_ESCOLHA", "DSC_CIDADE", "DSC_SUSEP_CORRETOR_PADRONIZADO",\
            "DSC_ABREVIACAO_UF", "DSC_PLACA", "IDT_BLINDADO", "TPO_VEICULO", "DSC_MODELO_VEICULO", "NUM_CHASSI", "DSC_NOME_CORRETORA", "DSC_CODIGO_FIPE", "DSC_COR_MATRIZ"]

cod_cols = ["COD_SEGURADORA", "DOC_CPF_CNPJ", "DOC_CPF_CNPJ_NORMATIZADO", "COD_FIPE_SEGURADORA", "NUM_APOLICE", "COD_ITEM", "COD_EXTERNO", \
            "COD_CATEGORIA", "COD_PRODUTO", "COD_PLANO", "COD_ANEXO", "COD_CORRETOR", "NUM_CORRETOR", "NUM_ANO_MODELO", "END_CEP"]

double_cols = ["VAL_VEICULO_INICIO", "VAL_VEICULO_ATUAL"]

date_cols = ["DAT_INICIO_VIGENCIA", "DAT_FIM_VIGENCIA", "DAT_INICIO_VIGENCIA_ORIGINAL"]


for col_name in rep.columns:
    
    if col_name in str_cols:
        rep = rep.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ''), lit('N/D')).otherwise(col(col_name)).cast("string"))
    
    elif col_name in cod_cols:
        rep = rep.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ''), lit('0')).otherwise(col(col_name)).cast("string"))    
    
    elif col_name in double_cols:
        rep = rep.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ""), lit(0)).otherwise(col(col_name)).cast("double"))
    
    elif col_name in date_cols:
        rep = rep.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ""), lit("1800-01-01")).otherwise(to_date(col(col_name), "dd/MM/yyyy")).cast("date"))

log.info(f"TRATAMENTO DE NULO CARREGADA")


# --------- WRITE TABLE --------- #

log.info(f"Iniciando escrita da tabela final no caminho: {path_table}")

rep.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path_table)

log.info("Escrita da tabela final concluida com sucesso!")

delta_table = DeltaTable.forPath(spark, path_table)

delta_table.vacuum(72)

delta_table.generate("symlink_format_manifest")
log.info("Manifesto criado com sucesso!")