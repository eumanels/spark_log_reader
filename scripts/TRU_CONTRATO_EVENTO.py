# Governance
from governance_utils import *

# Utils
from utils.zoro import DeltaLake
from delta.tables import DeltaTable
from utils_maxpar.data_clean import treat_cod, treat_dsc


# Variáveis

path_s3 = "s3://"

bucket  = "{{repositorioS3}}-{{ambiente}}"


# Paths
RAW_S3_URI = f"{path_s3}{bucket}/RAW/MAXPAR/AWS_GENESISAPOLICESCONTRATOS/RAW_CONTRATOEVENTOAGRUPAMENTO_GENESIS_CONTRATOS"
print(RAW_S3_URI)

TRU_S3_URI = f"{path_s3}{bucket}/SANDBOX/MAXPAR/TRUSTED/TRU_CONTRATO_EVENTO_AGRUPAMENTO_GENESIS_CONTRATOS_SANDBOX"
print(TRU_S3_URI)



# Mapeamento dos campos

MAPPING = {
    "ID": {
        "type": "string",
        "new_name": "COD_IDENTIFICADOR",
        "fill_na": "0",
        "business_rule": "",
    },
    "DESCRICAO": {
        "type": "string",
        "new_name": "TPO_CONTRATO_EVENTO_AGRUPAMENTO",
        "fill_na": "N/D",
        "business_rule": "",
    },
    "LIMITEID": {
        "type": "string",
        "new_name": "COD_LIMITE",
        "fill_na": "0",
        "business_rule": "",
    },
}


# Funções Extras

def extra_apply_business_rules(df):
    # Upper
    df = df.withColumn("DSC_CONTRATO_EVENTO_AGRUPAMENTO", F.upper(F.col("DSC_CONTRATO_EVENTO_AGRUPAMENTO")))

    return df


# Spark Session
spark = get_spark_session()

# Inicia o Delta Lake
dl = DeltaLake()


# Leitura tabela
df_raw = spark.read.format("delta").load(RAW_S3_URI).select(list(MAPPING.keys()))
dl.print_log(msg = "Tabela RAW lida com Sucesso\n")


# Tratamentos
df = cast_types(df_raw, MAPPING)

df = fill_na(df, MAPPING)

df = apply_business_rules(df, MAPPING)

df = rename_columns(df, MAPPING)

df = extra_apply_business_rules(df)

df = df.withColumns({
    "COD_IDENTIFICADOR":               treat_cod("COD_IDENTIFICADOR"),
    "COD_LIMITE":                      treat_cod("COD_LIMITE"),
    "DSC_CONTRATO_EVENTO_AGRUPAMENTO": treat_dsc("DSC_CONTRATO_EVENTO_AGRUPAMENTO", basico=True),
})

dl.print_log(msg = "Tratamentos concluídos\n")


# Salva a tabela

dl.print_log(msg = "Inicio do full load")

df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .save(TRU_S3_URI)

dl.print_log(msg = "Full Load Executado com Sucesso")


# Faz a limpeza dos arquivos antigos
delta_table = DeltaTable.forPath(spark, TRU_S3_URI)
delta_table.vacuum(0)
dl.print_log(msg = "Vacuum Executado com Sucesso")



# Gera o manifesto para a tabela Delta
dl.manifesto(TRU_S3_URI)

# Registra meta individual
dl.data_ingestao(TRU_S3_URI)
 