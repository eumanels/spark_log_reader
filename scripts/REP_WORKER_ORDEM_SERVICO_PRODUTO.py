​# ============ BIBLIOTECAS PYSPARK ============ #
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.zoro import *
from utils_maxpar.funcoes import valida_atualizacao_dependencias
from utils_maxpar import classe_decisor_incidentes
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# ============ VARIAVEIS ============ #

eng = 'LUCAS_CODECO, ARIOSTO_BRASILEIRO'
path_s3 = "s3://"
bucket = "{{repositorioS3}}-{{ambiente}}"

rep_table = "REP_WORKER_ORDEM_SERVICO"


# ============ SPARK SESSION E CONFIGURACOES ============ #

configs = [("spark.databricks.delta.retentionDurationCheck.enabled", False)]

dl = DeltaLake(app_name = rep_table, configs=configs)
spark = dl.build()
decisor = classe_decisor_incidentes.DecisorIncidentes(spark)


# ============ PATH TABELA ============ #

path_s3_bucket = f"{path_s3}{bucket}"

tabela_final_path = f"{path_s3_bucket}/REPORT/MAXPAR/ATENDIMENTO/"

full_path_table = tabela_final_path + rep_table

# ============ ATENDIMENTO ============ #

path_atendimento = f"{path_s3_bucket}/TRUSTED/MAXPAR/ATENDIMENTO/"

tru_atendimento_maxpar_path = path_atendimento + "TRU_ATENDIMENTO_MAXPAR"

tru_ordem_servico_maxpar_path = path_atendimento + "TRU_ORDEM_SERVICO_MAXPAR"

tru_ordem_servico_contato_maxpar_path = path_atendimento + "TRU_ORDEM_SERVICO_CONTATO_MAXPAR"

tru_ordem_servico_produto_maxpar_path = path_atendimento + "TRU_ORDEM_SERVICO_PRODUTO_MAXPAR"

tru_servico_maxpar_path = path_atendimento + "TRU_SERVICO_MAXPAR"

# ============ SCRIPT ============ #

tru_script_path = f"{path_s3_bucket}/TRUSTED/MAXPAR/APOLICE/TRU_SCRIPT_ATENDIMENTO_MAXPAR"

# ============ CLIENTE ============ #
path_cliente_maxpar = f"{path_s3_bucket}/TRUSTED/MAXPAR/CLIENTE/"

tru_seguradora_maxpar = path_cliente_maxpar + "TRU_SEGURADORA_MAXPAR"

# ============ MASTER ============ #

path_master = f"{path_s3_bucket}/TRUSTED/MASTER/"

tru_produto_path = path_master + "TRU_PRODUTO"

tru_empresa_path = path_master + "TRU_EMPRESA"

tru_usuario_path = path_master + "TRU_USUARIO_GENERICO"

tru_localizacao_path = path_master + "TRU_LOCALIZACAO"

tru_cliente_path = path_master + "TRU_CLIENTE"



# ============ VALIDA INGESTÃO ============ #

paths_apoio = [
    tru_atendimento_maxpar_path,
    tru_ordem_servico_maxpar_path,
    tru_ordem_servico_contato_maxpar_path,
    tru_ordem_servico_produto_maxpar_path
]

paths_apoio_nao_valida = [
    tru_produto_path,
    tru_empresa_path,
    tru_usuario_path,
    tru_localizacao_path,
    tru_cliente_path,
    tru_seguradora_maxpar,
    tru_script_path,
    tru_servico_maxpar_path
]

if "{{ambiente}}" == "prod-us-1":
    decisor.decisor_de_incidentes(paths_apoio + paths_apoio_nao_valida)
    for p in paths_apoio:
        valida_atualizacao_dependencias(p)

    for p in paths_apoio_nao_valida:
        valida_atualizacao_dependencias(p, valida_ingestao=False)


# ============ LEITURA DE TABELAS ============ #

filtro_tru_atendimento = ["NUM_APOLICE","COD_ITEM","COD_SEGURADORA","COD_ATENDIMENTO","COD_CIDADE","COD_CONTRATO","COD_ANEXO","COD_EXTERNO",
                                "DAT_INICIAL","IDT_CANCELADO","IDT_ORDEM_REEMBOLSO","DSC_OBJETO_CAUSA","COD_EVENTO","DSC_CHASSI","NUM_ANO_MODELO",
                                "DSC_PLACA","IDT_POLIDOR_FAROL","COD_SCRIPT","COD_TIPO_SCRIPT","IDT_REQUER_FRANQUIA","VAL_FRANQUIA",
                                "VAL_FRANQUIA_EVENTO","VAL_SALDO_FRANQUIA","VAL_FRANQUIA_PAGO"]

tru_atendimento = spark.read.format("delta").load(tru_atendimento_maxpar_path).select(filtro_tru_atendimento)

log.info("TRU_ATENDIMENTO CARREGADA")


filtro_tru_os = ["COD_ATENDIMENTO","COD_CLIENTE","DAT_ATENDIMENTO","COD_ORDEM_SERVICO","TPO_TROCA_REPARO","VAL_FRANQUIA",
                                "IDT_TABELA_PRECO","IDT_COMPRA_PROPRIA","IDT_ATACADO_SINISTRO","IDT_ATACADO_SINISTRO_AUTOMATICA","IDT_FORNECIMENTO_DIRETO"]

tru_os = spark.read.format("delta").load(tru_ordem_servico_maxpar_path).select(filtro_tru_os)

log.info("TRU_ORDEM_SERVICO CARREGADA")


filtro_tru_os_contato = ["COD_CONTATO","DSC_CONTATO","COD_ORDEM_SERVICO","DAT_FINAL", "COD_USUARIO"]

tru_os_contato = spark.read.format("delta").load(tru_ordem_servico_contato_maxpar_path).select(filtro_tru_os_contato)

log.info("TRU_ORDEM_SERVICO_CONTATO CARREGADA")


filtro_tru_os_produto = ["VAL_NEGOCIO","QTD_PRODUTO","COD_ORDEM_SERVICO","IDT_ESQUEMA","COD_PRODUTO","VAL_TARGET","VAL_FRANQUIA",
                                            "IDT_FORNECIMENTO_DIRETO"]

tru_os_produto = spark.read.format("delta").load(tru_ordem_servico_produto_maxpar_path).select(filtro_tru_os_produto)

log.info("TRU_ORDEM_SERVICO_PRODUTO CARREGADA")


filtro_tru_servico = ["COD_ORDEM_SERVICO","VAL_NEGOCIO","DSC_SERVICO"]

tru_servico = spark.read.format("delta").load(tru_servico_maxpar_path).select(filtro_tru_servico)

log.info("TRU_SERVICO_MAXPAR CARREGADA")


filtro_script_maxpar = ["COD_SCRIPT","COD_TIPO_SCRIPT","COD_CLASSIFICA_SCRIPT","DSC_SCRIPT","COD_GRUPO_TIPO_SCRIPT","DSC_GRUPO_TIPO_SCRIPT"]

tru_script = spark.read.format("delta").load(tru_script_path).select(filtro_script_maxpar)

log.info("TRU_SCRIPT CARREGADA")


filtro_tru_produto = ["COD_PRODUTO","DSC_PRODUTO","COD_TIPO_DESCRICAO","COD_SEMELHANCA","DSC_SEMELHANCA","COD_EQUIVALENCIA","DSC_EQUIVALENCIA"]

tru_produto = spark.read.format("delta").load(tru_produto_path).select(filtro_tru_produto).filter(col("COD_TIPO_DESCRICAO").cast("int") == 1).drop('COD_TIPO_DESCRICAO')

log.info("TRU_PRODUTO CARREGADA")


filtro_tru_seguradora = ["DSC_SEGURADORA","COD_SEGURADORA"]

tru_seguradora = spark.read.format("delta").load(tru_seguradora_maxpar).select(filtro_tru_seguradora)

log.info("TRU_SEGURADORA CARREGADA")


filtro_tru_empresa = ["NUM_SEQUENCIA","COD_CLIENTE","NUM_SEQUENCIA"]

tru_empresa = spark.read.format("delta").load(tru_empresa_path).select(filtro_tru_empresa)

log.info("TRU_EMPRESA CARREGADA")


filtro_tru_usuario = ["COD_USUARIO", "NOM_COMPLETO"]

tru_usuario = spark.read.format("delta").load(tru_usuario_path).select(filtro_tru_usuario).dropDuplicates()

log.info("TRU_USUARIO CARREGADA")


filtro_tru_localizacao = ["COD_CIDADE","NOM_CIDADE","COD_UF"]

tru_localizacao = (spark.read.format("delta").load(tru_localizacao_path)
                   .filter((col("NOM_CIDADE") != "N/D") & (~col("COD_UF").isin("NC", "UR", "EX")))
                   .select(filtro_tru_localizacao)
                   .dropDuplicates())

log.info("TRU_LOCALIZACAO CARREGADA")


filtro_tru_cliente = ["COD_CLIENTE","NOM_CLIENTE","DOC_CNPJ","COD_CIDADE"]

tru_cliente = spark.read.format("delta").load(tru_cliente_path).select(filtro_tru_cliente).dropDuplicates()

log.info("TRU_CLIENTE CARREGADA")


# ============ PADRONIZAÇÃO DE SCRIPT E PRODUTO ============ #

def normalizar_script(coluna):
    return upper(regexp_replace(translate(coluna,"ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ","AAAAEEEIIIOOOOUUUC"),"[^A-Za-z0-9 ]",""))

def aplicar_regras_otimizado(dsc_col, default="FAST_TRACK"):
    is_vid   = dsc_col.contains("VID")
    is_farol = dsc_col.contains("FAROL") | dsc_col.contains("FAROIS")
    is_lant  = dsc_col.contains("LANT") | dsc_col.contains("LUZ")
    is_ret   = dsc_col.contains("RETROV")
    is_outros = (
        dsc_col.contains("ADAS") |
        dsc_col.contains("CLIMAT") |
        dsc_col.contains("PARACHOQUE") |
        dsc_col.contains("SRA") |
        dsc_col.contains("PNEU") |
        dsc_col.contains("RODA")
    )

    return (
        when(is_vid,
when(dsc_col.rlike("MAQUINA DE VIDRO"), "MAQUINA_DE_VIDRO")
.when((dsc_col.rlike("VIDRO|PELICULA|GUARNICAO|PALHETA|GRAVACAO DE CHASSI")) & (~dsc_col.rlike("MAQUINA DE VIDRO")), "VIDRO"))
.when(is_farol,when(dsc_col.rlike("FAROL|FAROIS.*AUX"), "FAROL AUXILIAR").otherwise("FAROL"))
.when(is_lant,when(dsc_col.rlike("LANT|LUZ DE FREIO.*AUX"), "LANTERNA AUXILIAR").otherwise("LANTERNA"))
.when(is_ret,when(dsc_col.rlike("RETROV"), "RETROVISOR"))
.when(is_outros,

when(dsc_col.rlike("ADAS"), "ADAS")
.when(dsc_col.rlike("CLIMAT"), "CLIMATIZACAO")
.when(dsc_col.rlike("PARACHOQUE.*TROCA"), "TROCA_PARACHOQUE")
.when(dsc_col.rlike("PARACHOQUE"), "PARACHOQUE")
.when(dsc_col.rlike("SRA"), "SRA")
.when(dsc_col.rlike("MARTELINHO|REPARO DE AMASSADO"), "SRA_PLUS")
.when(dsc_col.rlike("COBERTURA CONTRA BURACOS|PNEU|RODA"), "RODA_PNEU_E_SUSPENSAO")
.when(dsc_col.rlike("TETO SOLAR"), "TETO_SOLAR")
)
        .otherwise(default)
    )

def aplicar_regras_mvr(dsc_col):
    is_vid   = dsc_col.rlike(r"^VID |^ENC VID")
    is_farol = dsc_col.rlike(r"^FAROL |^ENC FAROL")
    is_lant  = dsc_col.rlike(r"^LANT |^ENC LANT")
    is_ret   = dsc_col.substr(1, 25).contains("RETROV ")
    is_pch   = dsc_col.rlike(r"^PCHOQUE |^ENC PCHOQUE")
    is_pneu  = dsc_col.startswith("PNEU") & (~dsc_col.contains("ARO"))
    is_maq   = dsc_col.contains("MAQ VID") & (~dsc_col.startswith("MKTP"))

    return (
        # ---------------- VIDRO ----------------
        when(is_vid,
            when(dsc_col.rlike(" PB "),
                when(dsc_col.rlike(" CAM "), "PARABRISA CARGA").otherwise("PARABRISA"))
            .when(dsc_col.rlike(" VG "),
                when(dsc_col.rlike(" CAM "), "VIGIA CARGA").otherwise("VIGIA"))
            .when(dsc_col.rlike(" PTA "),
                when(dsc_col.rlike(" CAM "), "VIDRO PORTA CARGA").otherwise("VIDRO PORTA"))
            .when(dsc_col.rlike(" JAN "),
                when(dsc_col.rlike(" CAM "), "VIDRO JANELA CARGA").otherwise("VIDRO JANELA"))
            .when(dsc_col.rlike(" TETO SOLAR "), "TETO SOLAR")
        )
        # ---------------- FAROL ----------------
        .when(is_farol,
            when(dsc_col.rlike(" PRINC "), "FAROL PRINCIPAL")
            .when(dsc_col.rlike(" DIURNO "), "FAROL DIURNO")
            .otherwise("FAROL AUX")
        )
        # ---------------- LANTERNA ----------------
        .when(is_lant,
            when(dsc_col.rlike(" AUX "), "LANTERNA AUXILIAR")
            .when(dsc_col.rlike(" PCHOQUE "), "LANTERNA PCHOQUE")
            .when(dsc_col.rlike(" MALA "), "LANTERNA MALA")
            .when(dsc_col.rlike(" TR INF "), "LANTERNA TRASEIRA INFERIOR")
            .when(dsc_col.rlike(" TR SUP "), "LANTERNA TRASEIRA SUPERIOR")
            .when(dsc_col.rlike(" TR "), "LANTERNA TRASEIRA")
            .when(dsc_col.rlike(" DT "), "LANTERNA DIANTEIRA")
            .otherwise("LANTERNA (GERAL)")
        )
        # ---------------- RETROVISOR ----------------
        .when(is_ret,
            when(dsc_col.rlike("CARCAÇA RETROV"), "CARCAÇA RETROV.")
            .when(dsc_col.rlike("CAPA RETROV"), "CAPA RETROV.")
            .when(dsc_col.rlike("ARO RETROV"), "ARO RETROV.")
            .when(dsc_col.rlike("PISCA RETROV"), "PISCA RETROV.")
            .when(dsc_col.rlike("LENTE | ESP "), "LENTE RETROV.")
            .when(dsc_col.rlike("MOTOR RETROV"), "MOTOR RETROV.")
            .when(dsc_col.rlike("SIDE ASSIST"), "SIDE ASSIST RETROV.")
            .when(dsc_col.rlike("BASE RETROV"), "BASE RETROV.")
            .when(dsc_col.rlike("CAPA BRACO"), "CAPA BRACO RETROV.")
            .when(dsc_col.rlike("CAPA VEDAÇÃO"), "CAPA VEDAÇÃO RETROV.")
            .when(dsc_col.rlike("CAMERA RETROV"), "CAMERA RETROV.")
            .when(dsc_col.rlike("CHICOTE RETROV"), "CHICOTE RETROV.")
            .when(dsc_col.rlike("MODULO RETROV"), "MODULO RETROV.")
            .when(dsc_col.rlike("BRAÇO RETROV"), "BRAÇO RETROV.")
            .when(dsc_col.rlike("PARAFUSO RETROV"), "PARAFUSO RETROV.")
            .when(dsc_col.rlike("ANEL RETROV"), "LUZ CORTESIA RETROV.")
            .when(dsc_col.rlike("LUZ CORTESIA"), "LUZ CORTESIA RETROV.")
            .when(dsc_col.rlike("LUZ POSIC"), "LUZ POSIC RETROV.")
            .when(dsc_col.rlike("ACABAMENTO"), "ACABAMENTO RETROV.")
            .when(dsc_col.rlike("ESP RETROV"), "LENTE RETROV.")
            .when(dsc_col.rlike("GAR RETROV"), "GARRA RETROV.")
            .otherwise("RETROVISOR COMPLETO")
        )
        # ---------------- PARACHOQUE ----------------
        .when(is_pch,
            when(dsc_col.rlike(" LISO | PRIMER "),
                when(dsc_col.rlike(" INF "), "PARACHOQUE LISO INFERIOR")
                .when(dsc_col.rlike(" CENT "), "PARACHOQUE LISO CENTRAL")
                .when(dsc_col.rlike(" SUP "), "PARACHOQUE LISO SUPERIOR")
                .otherwise("PARACHOQUE LISO"))
            .when(dsc_col.rlike(" TEXT "),
                when(dsc_col.rlike(" INF "), "PARACHOQUE TEXTURIZADO INFERIOR")
                .when(dsc_col.rlike(" CENT "), "PARACHOQUE TEXTURIZADO CENTRAL")
                .when(dsc_col.rlike(" SUP "), "PARACHOQUE TEXTURIZADO SUPERIOR")
                .otherwise("PARACHOQUE TEXTURIZADO"))
            .when(dsc_col.rlike(" CROM "),
                when(dsc_col.rlike(" INF "), "PARACHOQUE CROMADO INFERIOR")
                .when(dsc_col.rlike(" CENT "), "PARACHOQUE CROMADO CENTRAL")
                .when(dsc_col.rlike(" SUP "), "PARACHOQUE CROMADO SUPERIOR")
                .otherwise("PARACHOQUE CROMADO"))
            .when(dsc_col.rlike(" INF "), "PARACHOQUE INFERIOR")
            .when(dsc_col.rlike(" SUP "), "PARACHOQUE SUPERIOR")
            .otherwise("PARACHOQUE")
        )
        # ---------------- OUTROS ----------------
        .when(is_pneu, "PNEU")
        .when(is_maq, "MAQ VID")
        # ---------------- DEFAULT ----------------
        .otherwise("ITEM EXTRA OS")
    )

tru_produto = (tru_produto
               .withColumn("DSC_SCRIPT_MVR", aplicar_regras_mvr(col("DSC_PRODUTO")))
               .drop('COD_TIPO_DESCRICAO'))

tru_produto.cache()

dsc_script_normalizado = normalizar_script(col("DSC_SCRIPT"))

tru_script = (tru_script
              .withColumn("DSC_SCRIPT_AJUSTADO", dsc_script_normalizado)
              .withColumn("DSC_SCRIPT_COBERTURA", aplicar_regras_otimizado(dsc_script_normalizado))
) 

tru_script.cache()


ordem_servico_produto_maxpar = (tru_os_produto
.select("COD_PRODUTO", "VAL_NEGOCIO", "QTD_PRODUTO", "COD_ORDEM_SERVICO", "IDT_ESQUEMA")
.dropDuplicates(["COD_ORDEM_SERVICO", "COD_PRODUTO"])
)

# ============ CUSTO_PRODUTO ============ #

base_custo_produto = (tru_atendimento.alias("ATD")
    .join(tru_os.alias("OSM"), 
col("ATD.COD_ATENDIMENTO") == col("OSM.COD_ATENDIMENTO"), "left")

    .join(tru_os_contato.alias("OSC"), 
col("OSM.COD_ORDEM_SERVICO") == col("OSC.COD_ORDEM_SERVICO"), "left")

.join(ordem_servico_produto_maxpar.alias("OSP"), 
col("OSM.COD_ORDEM_SERVICO") == col("OSP.COD_ORDEM_SERVICO"), "left")

    .join(broadcast(tru_empresa).alias("EMP"), 
(col("OSM.COD_CLIENTE") == col("EMP.COD_CLIENTE"))
& (col("EMP.NUM_SEQUENCIA") == lit(999)), "left")

    .join(broadcast(tru_script).alias("SCR"), 
(col("ATD.COD_SCRIPT") == col("SCR.COD_SCRIPT"))
& (col("ATD.COD_TIPO_SCRIPT") == col("SCR.COD_TIPO_SCRIPT")), "left")

    .groupBy(
        col("ATD.NUM_APOLICE"),
        col("ATD.COD_ITEM"),
        col("ATD.COD_SEGURADORA"),
        col("OSM.COD_CLIENTE"),
        col("ATD.COD_ATENDIMENTO"),
        col("ATD.COD_CIDADE"),
        col("ATD.COD_CONTRATO"),
        col("ATD.COD_ANEXO"),
        col("ATD.DSC_CHASSI"),
        col("ATD.NUM_ANO_MODELO"),
        col("ATD.COD_EXTERNO"),
        col("ATD.IDT_CANCELADO"),
        col("ATD.IDT_ORDEM_REEMBOLSO"),
        col("ATD.DSC_OBJETO_CAUSA"),
        col("ATD.COD_EVENTO"),
        col("ATD.DAT_INICIAL").alias("DAT_ATENDIMENTO"),
        col("OSM.COD_ORDEM_SERVICO"),
        col("OSM.TPO_TROCA_REPARO"),
        col("OSM.IDT_TABELA_PRECO"),
        col("OSM.IDT_COMPRA_PROPRIA"),
        col("OSM.IDT_ATACADO_SINISTRO"),
        col("OSM.IDT_ATACADO_SINISTRO_AUTOMATICA"),
        col("ATD.IDT_POLIDOR_FAROL"),
        col("ATD.DSC_PLACA"),
        col("SCR.DSC_SCRIPT"),
col("SCR.DSC_SCRIPT_AJUSTADO"),
col("SCR.DSC_SCRIPT_COBERTURA"),
        col("SCR.COD_CLASSIFICA_SCRIPT"),
        col("SCR.COD_GRUPO_TIPO_SCRIPT"),
        col("SCR.DSC_GRUPO_TIPO_SCRIPT"),
        when(col("EMP.NUM_SEQUENCIA") == "999", lit("LOJA PROPRIA")).otherwise(lit("CREDENCIADO")).alias("IDT_LOJA")
    )
    .agg(
        sum(# VAL_DESPESA_PRODUTO_7
            when(col("OSC.COD_CONTATO") == 7, coalesce(col("OSP.VAL_NEGOCIO"), lit(0)) * coalesce(col("OSP.QTD_PRODUTO"), lit(1)))
        ).alias("VAL_DESPESA_PRODUTO_7"),

        sum(# VAL_DESPESA_PRODUTO_2
            when(col("OSC.COD_CONTATO") == 2, coalesce(col("OSP.VAL_NEGOCIO"), lit(0)) * coalesce(col("OSP.QTD_PRODUTO"), lit(1)))
        ).alias("VAL_DESPESA_PRODUTO_2"),
    )
)
log.info(f"BASE_CUSTO_PRODUTO Criada")

# ============ CUSTO_SERVICO ============ #

base_custo_servico = (tru_atendimento.alias("ATD")
.join(tru_os.alias("OSM"), 
col("ATD.COD_ATENDIMENTO") == col("OSM.COD_ATENDIMENTO"), "left")

.join(tru_os_contato.alias("OSC"), 
col("OSM.COD_ORDEM_SERVICO") == col("OSC.COD_ORDEM_SERVICO"), "left")

.join(tru_servico.alias("SERV"), 
col("OSM.COD_ORDEM_SERVICO") == col("SERV.COD_ORDEM_SERVICO"), "left")

    .groupBy(
        col("ATD.COD_ATENDIMENTO"), 
col("OSM.COD_ORDEM_SERVICO"), 
col("OSM.VAL_FRANQUIA")
    )
    .agg(
        # -------- VALORES --------
        sum( ### SUM CASE WHEN COD_CONTATO = 7
            when(col("OSC.COD_CONTATO") == 7, coalesce(col("SERV.VAL_NEGOCIO"), lit(0)))
        ).alias("VAL_DESPESA_SERVICO_7"),

        sum( ### SUM CASE WHEN COD_CONTATO = 2
            when(col("OSC.COD_CONTATO") == 2, coalesce(col("SERV.VAL_NEGOCIO"), lit(0)))
        ).alias("VAL_DESPESA_SERVICO_2"),

        # -------- DATAS --------
        max( ### MAX DATA CONTATO 7
            when(col("OSC.COD_CONTATO") == 7, col("OSC.DAT_FINAL")).otherwise(to_date(lit("1800-01-01")))
        ).alias("DAT_REALIZACAO_OS"),

        max( ### MAX DATA CONTATO 2
            when(col("OSC.COD_CONTATO") == 2, col("OSC.DAT_FINAL")).otherwise(to_date(lit("1800-01-01")))
        ).alias("DAT_CRIACAO_OS"),
        
max_by( ### DSC_CONTATO da maior DAT_FINAL do atendimento
            col("OSC.DSC_CONTATO"), col("OSC.DAT_FINAL")
        ).alias("IDT_STATUS_ORDEM_SERVICO")
    )
)
log.info(f"BASE_CUSTO_SERVICO Criada")


# ============ PRODUTO PRINCIPAL ============ #

produto_principal = (tru_os_produto.alias("PRO")
# ---------------- JOIN TARGET ZERO ----------------
    .join((tru_os_produto
.groupBy("COD_ORDEM_SERVICO").agg(max(
when((col("VAL_TARGET") == 0) | col("VAL_TARGET").isNull(), lit(1)).otherwise(lit(0))).alias("TEM_TARGET_ZERO")
)
).alias("OTG"), 
col("PRO.COD_ORDEM_SERVICO") == col("OTG.COD_ORDEM_SERVICO"), "left")

.select(
col("PRO.COD_ORDEM_SERVICO"),
col("PRO.COD_PRODUTO").alias("COD_PRODUTO_PRINCIPAL"),
coalesce(col("OTG.TEM_TARGET_ZERO"), lit(0)).cast("boolean").alias("TEM_TARGET_ZERO"),
col("PRO.VAL_NEGOCIO"),
col("PRO.VAL_TARGET")
)
.withColumn("ORDENACAO", row_number().over(
        Window.partitionBy("COD_ORDEM_SERVICO")
.orderBy(
when(col("TEM_TARGET_ZERO"), col("VAL_NEGOCIO"))
.otherwise(col("VAL_TARGET"))
.desc()
)
)
).filter(col("ORDENACAO") == 1)

# ---------------- JOIN PRODUTO ----------------
.join(tru_produto.alias("TPRO"),
col("TPRO.COD_PRODUTO") == col("COD_PRODUTO_PRINCIPAL"), "left")

# ---------------- SELECT ----------------
.select(
col("COD_ORDEM_SERVICO"),
col("COD_PRODUTO_PRINCIPAL"),
col("TPRO.DSC_PRODUTO"),
col("TPRO.DSC_SCRIPT_MVR"),
col("TPRO.COD_EQUIVALENCIA"),
col("TPRO.DSC_EQUIVALENCIA"),
col("TPRO.COD_SEMELHANCA"),
col("TPRO.DSC_SEMELHANCA")
)
)
log.info("PRODUTO_PRINCIPAL Criada")

# ============ VALOR_FRANQUIA ============ #

valor_franquia = (tru_os.alias("OSM")
    .join(tru_atendimento.alias("ATD"), col("ATD.COD_ATENDIMENTO") == col("OSM.COD_ATENDIMENTO"), "left")
    .join(tru_atendimento
.groupBy("COD_EVENTO").agg(count("*").alias("QTD_REGISTRO")).alias("EVEN"),
col("EVEN.COD_EVENTO") == col("ATD.COD_EVENTO"), "left")
    .join(tru_atendimento.filter(col("IDT_REQUER_FRANQUIA") == "SIM")
            .groupBy("COD_EVENTO").agg(max("VAL_FRANQUIA").alias("VAL_FRANQUIA")).alias("FME"),
        col("FME.COD_EVENTO") == col("ATD.COD_EVENTO"), "left")
    .join(tru_atendimento.alias("ATDM")
          .join(tru_os.alias("OSMM"),col("OSMM.COD_ATENDIMENTO") == col("ATDM.COD_ATENDIMENTO"), "left")
          .filter(col("ATDM.IDT_REQUER_FRANQUIA") == "SIM")
          .groupBy(col("ATDM.COD_EVENTO"), col("ATDM.VAL_FRANQUIA"))
          .agg(min(col("ATDM.COD_ATENDIMENTO")).alias("COD_ATENDIMENTO")).alias("EVATEMIN"),
          (col("EVATEMIN.COD_EVENTO") == col("FME.COD_EVENTO")) 
& (round(col("EVATEMIN.VAL_FRANQUIA"),2) == round(col("FME.VAL_FRANQUIA"),2)), "left")

    .select(
        col("OSM.COD_ORDEM_SERVICO"),
        col("OSM.IDT_FORNECIMENTO_DIRETO").alias("IDT_ORDEM_SERVICO_FORNECIMENTO_DIRETO"),
        when(
            when(coalesce(col("EVEN.QTD_REGISTRO"), lit(0)).isin(0,1), lit("NÃO"))
            .otherwise(lit("SIM")) != "SIM",
            coalesce(col("OSM.VAL_FRANQUIA"), col("ATD.VAL_FRANQUIA"))
        )
        .otherwise(
            when(col("ATD.VAL_FRANQUIA_PAGO") > 0, col("ATD.VAL_FRANQUIA_PAGO"))
            .otherwise(
                when(col("EVATEMIN.COD_ATENDIMENTO") == 0, lit(0))
                .otherwise(
                    when(col("EVATEMIN.COD_ATENDIMENTO") == col("ATD.COD_ATENDIMENTO"),
                        when(col("ATD.VAL_FRANQUIA_EVENTO") == col("ATD.VAL_SALDO_FRANQUIA"), col("ATD.VAL_FRANQUIA_EVENTO"))
                        .when(col("ATD.VAL_SALDO_FRANQUIA") > 0, col("ATD.VAL_SALDO_FRANQUIA"))
                        .otherwise(lit(0)))
                    .when((col("ATD.VAL_FRANQUIA_EVENTO") != col("ATD.VAL_SALDO_FRANQUIA")) & (col("ATD.VAL_SALDO_FRANQUIA") > 0), col("ATD.VAL_SALDO_FRANQUIA"))
                    .otherwise(lit(0))
                )
            )
        ).alias("VAL_FRANQUIA")
    )
)
log.info("VALOR_FRANQUIA Criada")


# ============ ORDEM CONTATO ============ #

u_cri = tru_usuario.alias("u_cri")
u_rea = tru_usuario.alias("u_rea")

tru_os_contato_piv = (tru_os_contato
    .groupBy(
        col('COD_ORDEM_SERVICO'),
    ).agg(
        max(when(col('COD_CONTATO') == '2', col('COD_USUARIO'))).alias("COD_USUARIO_CRIACAO"),
        max(when(col('COD_CONTATO') == '7', col('COD_USUARIO'))).alias("COD_USUARIO_REALIZACAO")
    ).join(
        u_cri,
        col("COD_USUARIO_CRIACAO") == col("u_cri.COD_USUARIO"),
        'left'
    ).join(
        u_rea,
        col("COD_USUARIO_REALIZACAO") == col("u_rea.COD_USUARIO"),
        'left'
    ).select(
        col('COD_ORDEM_SERVICO'),
        col('u_cri.NOM_COMPLETO').alias('NOM_USUARIO_CRIACAO'),
        col('u_rea.NOM_COMPLETO').alias('NOM_USUARIO_REALIZACAO')
    )
)
log.info(f"TRU_ORDEM_SERVICO_CONTATO_MAXPAR_PIVOT Criada")


# ============ TABELA FINAL ============ #

tabela_final = (base_custo_produto.alias("BP")
.join(base_custo_servico.alias("BS"), 
col("BP.COD_ATENDIMENTO") == col("BS.COD_ATENDIMENTO"), "left")

.join(produto_principal.alias("PP"), 
col("PP.COD_ORDEM_SERVICO") == col("BP.COD_ORDEM_SERVICO"), "left")

.join(tru_os_produto.alias("OSP"), 
(col("OSP.COD_ORDEM_SERVICO") == col("BP.COD_ORDEM_SERVICO"))
& (col("OSP.COD_PRODUTO") == col("PP.COD_PRODUTO_PRINCIPAL")), "left")

.join(valor_franquia.alias("FRANQ"),
col("BP.COD_ORDEM_SERVICO") == col("FRANQ.COD_ORDEM_SERVICO"), "left")
        
.join(broadcast(tru_seguradora).alias("SEG"), 
col("SEG.COD_SEGURADORA") == col("BP.COD_SEGURADORA"), "left")
        
.join(broadcast(tru_cliente).alias("CLI"), 
col("CLI.COD_CLIENTE") == col("BP.COD_CLIENTE"), "left")

.join(broadcast(tru_localizacao).alias("LOC_A"),
col("LOC_A.COD_CIDADE") == col("BP.COD_CIDADE"), "left")

.join(broadcast(tru_localizacao).alias("LOC_C"),
col("LOC_C.COD_CIDADE") == col("CLI.COD_CIDADE"), "left")
        
.join(tru_os_contato_piv.alias("OS_PIV"),
        (col("OS_PIV.COD_ORDEM_SERVICO")==col("OSP.COD_ORDEM_SERVICO")), "left")

    .select(
# ------- ATENDIMENTO ---------
col("BP.COD_ATENDIMENTO"),
col("BP.COD_ORDEM_SERVICO"),
col("BP.COD_SEGURADORA"),
col("SEG.DSC_SEGURADORA"), 
col("BP.NUM_APOLICE"),
col("BP.COD_ITEM"),
col("BP.COD_EVENTO"),
        col("OS_PIV.NOM_USUARIO_CRIACAO"),
        col("OS_PIV.NOM_USUARIO_REALIZACAO"),

# --------- CLIENTE ---------
col("BP.COD_CLIENTE"),
        col("CLI.NOM_CLIENTE"),
col("CLI.DOC_CNPJ"),
col("BP.IDT_LOJA"),    
        
# --------- CONTRATO ---------
col("BP.COD_CONTRATO"),
        
# --------- STATUS/IDT ---------
        col("BS.IDT_STATUS_ORDEM_SERVICO"),
when(year(col("BS.DAT_REALIZACAO_OS")) == 1800, "NÃO").otherwise("SIM").alias("IDT_OS_REALIZADA"),
col("BP.IDT_CANCELADO"),
col("BP.IDT_ORDEM_REEMBOLSO"),
col("BP.TPO_TROCA_REPARO"),
col("OSP.IDT_ESQUEMA").alias("IDT_FORNECIMENTO"),
col("OSP.IDT_FORNECIMENTO_DIRETO").alias("IDT_PRODUTO_FORNECIMENTO_DIRETO"),
col("BP.IDT_POLIDOR_FAROL"),
        
# --------- MODALIDADES NEGOCIACAO ---------
col("BP.IDT_TABELA_PRECO"),
col("BP.IDT_COMPRA_PROPRIA"),
col("BP.IDT_ATACADO_SINISTRO"),
col("BP.IDT_ATACADO_SINISTRO_AUTOMATICA"),
col("FRANQ.IDT_ORDEM_SERVICO_FORNECIMENTO_DIRETO"),
        
# --------- CEP/REGIAO ---------
when(col("BP.IDT_ORDEM_REEMBOLSO") == "SIM", col("LOC_A.COD_UF")).otherwise(col("LOC_C.COD_UF")).alias("COD_UF_REEMBOLSO"),
        col("LOC_A.COD_UF").alias("COD_UF_ATENDIMENTO"),
        col("LOC_A.NOM_CIDADE").alias("DSC_CIDADE_ATENDIMENTO"),
        col("LOC_C.COD_UF").alias("COD_UF_CLIENTE"),
        col("LOC_C.NOM_CIDADE").alias("DSC_CIDADE_CLIENTE"),
        
        # --------- DATAS ---------
col("BP.DAT_ATENDIMENTO"),
col("BS.DAT_REALIZACAO_OS"),
col("BS.DAT_CRIACAO_OS"),
        
# --------- VEICULO ---------
        col("BP.DSC_CHASSI"),
        col("BP.DSC_PLACA"),
        col("BP.NUM_ANO_MODELO"),
        
# --------- PRODUTO ---------
col("PP.COD_PRODUTO_PRINCIPAL"),
col("PP.DSC_PRODUTO"),

# --------- PRODUTO E/S ---------
col("PP.COD_EQUIVALENCIA"),
col("PP.DSC_EQUIVALENCIA"),
col("PP.COD_SEMELHANCA"),
col("PP.DSC_SEMELHANCA"),
col("BP.DSC_SCRIPT"),
col("BP.DSC_SCRIPT_AJUSTADO"),
col("BP.DSC_SCRIPT_COBERTURA"),
col("PP.DSC_SCRIPT_MVR"),
col("BP.COD_GRUPO_TIPO_SCRIPT"),
col("BP.DSC_GRUPO_TIPO_SCRIPT"),
col("BP.DSC_OBJETO_CAUSA"),
        
# --------- CUSTO/VALORES ---------
coalesce(col("BP.VAL_DESPESA_PRODUTO_7"), col("BP.VAL_DESPESA_PRODUTO_2")).alias("VAL_DESPESA_PRODUTO"),
col("OSP.VAL_NEGOCIO").alias("VAL_DESPESA_PRODUTO_PRINCIPAL"),
coalesce(col("BS.VAL_DESPESA_SERVICO_7"), col("BS.VAL_DESPESA_SERVICO_2")).alias("VAL_DESPESA_SERVICO"),

col("OSP.VAL_TARGET"),
col("FRANQ.VAL_FRANQUIA"),
        
(coalesce(col("BP.VAL_DESPESA_PRODUTO_7"), col("BP.VAL_DESPESA_PRODUTO_2")) +
coalesce(col("BS.VAL_DESPESA_SERVICO_7"), col("BS.VAL_DESPESA_SERVICO_2")) - col("FRANQ.VAL_FRANQUIA")
).alias("VAL_CUSTO_TOTAL_FINANCEIRO"),

(coalesce(col("BP.VAL_DESPESA_PRODUTO_7"), col("BP.VAL_DESPESA_PRODUTO_2")) +
coalesce(col("BS.VAL_DESPESA_SERVICO_7"), col("BS.VAL_DESPESA_SERVICO_2"))
).alias("VAL_CUSTO_TOTAL_FINANCEIRO_FRANQUIA")


        )
        .dropDuplicates(["COD_ATENDIMENTO", "COD_ORDEM_SERVICO", "COD_PRODUTO_PRINCIPAL"]))

log.info("TABELA_FINAL Criada")


# ============ TRATAMENTO DE NULOS ============ #

col_cast = {
    # ============ STRING - DSC/IDT/TPO ============ #
    "DSC_SEGURADORA" : ['string', 'N/D'],
    "NOM_CLIENTE" : ['string', 'N/D'],
    "NOM_USUARIO_CRIACAO" : ['string', 'N/D'],
    "NOM_USUARIO_REALIZACAO" : ['string', 'N/D'],
    "DOC_CNPJ" : ['string', 'N/D'],
    "IDT_LOJA" : ['string', 'N/D'],
    "IDT_STATUS_ORDEM_SERVICO" : ['string', 'N/D'],
    "IDT_OS_REALIZADA" : ['string', 'N/D'],
    "IDT_CANCELADO" : ['string', 'N/D'],
    "IDT_ORDEM_REEMBOLSO" : ['string', 'N/D'],
    "TPO_TROCA_REPARO" : ['string', 'N/D'],
    "IDT_FORNECIMENTO" : ['string', 'N/D'],
    "IDT_POLIDOR_FAROL" : ['string', 'N/D'],
    "IDT_TABELA_PRECO" : ['string', 'N/D'],
    "IDT_COMPRA_PROPRIA" : ['string', 'N/D'],
    "IDT_ATACADO_SINISTRO" : ['string', 'N/D'],
    "IDT_ATACADO_SINISTRO_AUTOMATICA" : ['string', 'N/D'],
    "DSC_CIDADE_ATENDIMENTO" : ['string', 'N/D'],
    "DSC_CIDADE_CLIENTE" : ['string', 'N/D'],
    "DSC_PLACA" : ['string', 'N/D'],
    "DSC_CHASSI" : ['string', 'N/D'],
    "NUM_ANO_MODELO" : ['string', 'N/D'],
    "DSC_PRODUTO" : ['string', 'N/D'],
    "DSC_EQUIVALENCIA" : ['string', 'N/D'],
    "DSC_SEMELHANCA" : ['string', 'N/D'],
    "DSC_SCRIPT" : ['string', 'N/D'],
    "DSC_SCRIPT_AJUSTADO" : ['string', 'N/D'],
    "DSC_SCRIPT_COBERTURA" : ['string', 'N/D'],
    "DSC_GRUPO_TIPO_SCRIPT" : ['string', 'N/D'],
    "DSC_OBJETO_CAUSA" : ['string', 'N/D'],
    "COD_UF_REEMBOLSO" : ['string', 'N/D'],
    "IDT_PRODUTO_FORNECIMENTO_DIRETO" : ['string', 'N/D'],
    "IDT_ORDEM_SERVICO_FORNECIMENTO_DIRETO" : ['string', 'N/D'],
    "DSC_SCRIPT_MVR" : ['string', 'N/D'],

    # ============ STRING - COD ============ #
    "COD_ATENDIMENTO" : ['string', '0'],
    "COD_ORDEM_SERVICO" : ['string', '0'],
    "COD_SEGURADORA" : ['string', '0'],
    "NUM_APOLICE" : ['string', '0'],
    "COD_ITEM" : ['string', '0'],
    "COD_EVENTO" : ['string', '0'],
    "COD_CLIENTE" : ['string', '0'],
    "COD_CONTRATO" : ['string', '0'],
    "COD_UF_ATENDIMENTO" : ['string', '0'],
    "COD_UF_CLIENTE" : ['string', '0'],
    "COD_PRODUTO_PRINCIPAL" : ['string', '0'],
    "COD_EQUIVALENCIA" : ['string', '0'],
    "COD_SEMELHANCA" : ['string', '0'],
    "COD_GRUPO_TIPO_SCRIPT" : ['string', '0'],

    # ============ DOUBLE - VAL ============ #
    "VAL_DESPESA_PRODUTO" : ['double', 0],
    "VAL_DESPESA_PRODUTO_PRINCIPAL" : ['double', 0],
    "VAL_DESPESA_SERVICO" : ['double', 0],
    "VAL_TARGET" : ['double', 0],
    "VAL_FRANQUIA" : ['double', 0],
    "VAL_CUSTO_TOTAL_FINANCEIRO" : ['double', 0],
    "VAL_CUSTO_TOTAL_FINANCEIRO_FRANQUIA" : ['double', 0],
    "VAL_LIMITE_MONETARIO" : ['double', 0],
    "VAL_NOTA_FISCAL_CREDENCIADO_ENCOMENDA" : ['double', 0],
    "VAL_IMPOSTO" : ['double', 0],
    "VAL_FRETE" : ['double', 0],

    # ============ TIMESTAMP - DAT ============ #
    "DAT_ATENDIMENTO" : ['timestamp', '1800-01-01 00:00:00'],
    "DAT_REALIZACAO_OS" : ['timestamp', '1800-01-01 00:00:00'],
    "DAT_CRIACAO_OS" : ['timestamp', '1800-01-01 00:00:00'],
    "DAT_CANCELAMENTO_APOLICE" : ['timestamp', '1800-01-01 00:00:00'],
}

exprs = []
for coluna in tabela_final.columns:
    dtype, default_null = col_cast[coluna]
    base_col = when(
                    col(coluna).isNull() | (col(coluna) == ''),
                    lit(default_null)
                ).otherwise(col(coluna))
    if dtype == 'timestamp':
        expr = to_timestamp(base_col).cast("timestamp").alias(coluna)
    elif dtype == 'date':
        expr = to_date(base_col).cast("date").alias(coluna)
    else:
        expr = base_col.cast(dtype).alias(coluna)
    exprs.append(expr)
tabela_final = tabela_final.select(*exprs)

colunas_ordenadas = [
    # -------- APOLICE/ATENDIMENTO ---------
    "COD_ATENDIMENTO", 
    "COD_ORDEM_SERVICO",
    "COD_SEGURADORA", 
    "DSC_SEGURADORA", 
    "NUM_APOLICE", 
    "COD_ITEM", 
    "COD_EVENTO",
    "NOM_USUARIO_CRIACAO",
    "NOM_USUARIO_REALIZACAO",

    # --------- CLIENTE ---------
    "COD_CLIENTE",
    "NOM_CLIENTE",
    "DOC_CNPJ",
    "IDT_LOJA",

    # --------- CONTRATO ---------
    "COD_CONTRATO",

    # --------- STATUS/IDT ---------
    "IDT_STATUS_ORDEM_SERVICO",
    "IDT_OS_REALIZADA", 
    "IDT_CANCELADO",
    "IDT_ORDEM_REEMBOLSO",
    "TPO_TROCA_REPARO",
    "IDT_FORNECIMENTO",
    "IDT_ORDEM_SERVICO_FORNECIMENTO_DIRETO",
    "IDT_POLIDOR_FAROL",

    # --------- MODALIDADES NEGOCIACAO ---------
    "IDT_TABELA_PRECO",
    "IDT_COMPRA_PROPRIA",
    "IDT_ATACADO_SINISTRO",
    "IDT_ATACADO_SINISTRO_AUTOMATICA",

    # --------- CEP/REGIAO ---------
    "COD_UF_REEMBOLSO",
    "COD_UF_ATENDIMENTO",
    "DSC_CIDADE_ATENDIMENTO",
    "COD_UF_CLIENTE",
    "DSC_CIDADE_CLIENTE",

    # --------- DATAS ---------
    "DAT_ATENDIMENTO",
    "DAT_REALIZACAO_OS",
    "DAT_CRIACAO_OS",

    # --------- VEICULO ---------
    "DSC_CHASSI", 
    "DSC_PLACA", 
    "NUM_ANO_MODELO",

    # --------- PRODUTO ---------
    "COD_PRODUTO_PRINCIPAL",
    "DSC_PRODUTO",
    "IDT_PRODUTO_FORNECIMENTO_DIRETO",

    # --------- PRODUTO E/S ---------
    "COD_EQUIVALENCIA",
    "DSC_EQUIVALENCIA",
    "COD_SEMELHANCA",
    "DSC_SEMELHANCA",

    # --------- SCRIPT ---------
    "DSC_SCRIPT",
    "DSC_SCRIPT_AJUSTADO", 
    "DSC_SCRIPT_COBERTURA",
    "DSC_SCRIPT_MVR",
    "COD_GRUPO_TIPO_SCRIPT", 
    "DSC_GRUPO_TIPO_SCRIPT", 
    "DSC_OBJETO_CAUSA", 
    
    # --------- CUSTO/VALORES ---------        
    "VAL_DESPESA_PRODUTO", 
    "VAL_DESPESA_PRODUTO_PRINCIPAL", 
    "VAL_DESPESA_SERVICO", 
    "VAL_TARGET",
    "VAL_FRANQUIA",
    "VAL_CUSTO_TOTAL_FINANCEIRO", 
    "VAL_CUSTO_TOTAL_FINANCEIRO_FRANQUIA",
]
tabela_final = tabela_final.select(*colunas_ordenadas)
log.info(f"TRATAMENTO DE NULO Realizado")

# ============ ESCRITA TABELA ============ #

log.info("Inicio da escrita")

tabela_final.write.format('delta').option('overwriteSchema', 'true').mode('overwrite').save(full_path_table)

log.info(f'Full Load executado em {full_path_table}')

delta_table = DeltaTable.forPath(spark, full_path_table)

delta_table.vacuum()
print('Vacuum executado com sucesso.')

log.info("DeltaTable Criado com Sucesso")

tru_produto.unpersist()
tru_script.unpersist()

log.info("Cache retirado da memória")

dl.manifesto(full_path_table)
log.info("Manifesto criado com sucesso")

dl.data_ingestao(full_path_table)