from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os


# /usr/lib/jvm/java-19-openjdk-amd64

# set Java home
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-19-openjdk-amd64"
os.environ['SPARK_LOCAL_HOSTNAME'] = "172.30.45.213"

conf = SparkConf() \
    .setAppName("Analise OC") \
    .setMaster("local[2]") \
    .set("spark.driver.extraClassPath", "~/dev_prod/indicadores_supply/.venv/lib64/python3.10/site-packages/pyspark/*") \
    .set("spark.driver.extraClassPath", "/home/celso/dev_prod/indicadores_supply/_driver/sqljdbc_12.6/ptb/jars/*")
# .set('spark.ui.showConsoleProgress', 'true')


sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)


print(spark.version)

host = '172.30.45.213:1444'
database = "Seel"
schema = 'dbo'
table = "vw_90_itemRM"
user = 'sa'
password = 'PasSw0rd'

jdbc_url = f'jdbc:sqlserver://{host};database={database};encrypt=true;trustServerCertificate=true;'

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"{schema}.{table}") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

colums = ['ProcessoCompra_Id', 'Id', 'NumeroRM', 'FASE_RM', 'STATUS_ITEM', 'StatusRel_ITEM',
          'SAPRegraDistribuicao1_Id', 'SAPCodRegraDistribuicao1_RM',
          'SAPDscRegraDistribuicao1_RM', 'SAPRegraDistribuicao2_Id', 'SAPCodRegraDistribuicao2_RM',
          'SAPDscRegraDistribuicao2_RM', 'SAPRegraDistribuicao3_Id', 'SAPCodRegraDistribuicao3_RM',
          'SAPDscRegraDistribuicao3_RM', 'SAPRegraDistribuicao4_Id', 'SAPCodRegraDistribuicao4_RM',
          'SAPDscRegraDistribuicao4_RM', 'SAPRegraDistribuicao5_Id', 'SAPCodRegraDistribuicao5_RM',
          'SAPDscRegraDistribuicao5_RM', 'CodMtv_RM', 'MOTIVO_RM', 'MotivoRequisicao_Id', 'CodMaterial_RM',
          'ReferenciaMaterial', 'Categoria_Id', 'CodCategoriaMat_RM', 'CategoriaMat_RM',
          'DescricaoUnidadeMaterial', 'DescCompletaUnidadeMaterial', 'SegmentoMat_RM', 'QtdAtual_RM',
          'Seq_RM', 'DesMaterial_RM', 'Qtde_RM', 'QtdeReserv_RM', 'QtdeAtd_RM', 'QtdeAtdOC_RM',
          'QtdeCancRC_RM', 'QtdeCanc_RM', 'QtdeCancOC_RM', 'QtdeCancMC_RM', 'Equipamento_Id',
          'CodEquip_RM', 'DesEquip_RM', 'EquipApelido', 'EspecieEquipamento', 'ChassiEquipamento',
          'NumeroSerieEquipamento', 'MarcaEquipamento', 'ModeloEquipamento', 'AnoFabricacaoEqp',
          'EquipamentoCentroCusto_Id', 'CodCentroCustoEquip', 'DescricaoCentroCustoEquip', 'Natureza_Id',
          'CodigoNatureza', 'DescricaoNatureza', 'CentroCustoApropriacao_Id', 'CodCentroCusto_RM',
          'DesCentroCusto_RM', 'CodigoFabricante_RM', 'Fabricante_RM', 'Equivalancia_RM', 'CodigoMotivoBaixa',
          'DescricaoMotivoBaixa', 'CodDst', 'DscDst', 'CodOri', 'DscOri', 'CodLocAplic', 'DscLocAplic',
          'Servico_Id', 'CodigoPlanilha', 'CodigoServico', 'DescricaoServico', 'SITUACAO_RM',
          'CodSubempreiteiro_RM', 'Subempreiteiro_RM', 'Numero_RC', 'Seq_RC', 'Numero_MC', 'Seq_MC',
          'Seq_OC', 'Numero_OC', 'Projeto', 'SAPUtilizacaoCodigo', 'SAPUtilizacaoDescricao', 'CodigoUnidadeSAP',
          'QuantidadeBase', 'QuantidadeUnidade', 'DataPrevisaoEntrega', 'DataPrevPresServico'
          ]

df1 = df[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024
# df1.show()

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
