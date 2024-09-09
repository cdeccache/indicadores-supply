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
table = "vw_90_processocompra"
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

colums = ['Id', 'Id_RM', 'Id_RC', 'Numero_RM',
          'SITUACAO_RM', 'FASE_RM', 'MOTIVO_RM',
          'DATA_RM', 'DATAENTREGA_RM', 'DataAssinatura_RM',
          'CodLocalOrigem', 'LocalOrigem', 'CodLocalAplicacao',
          'LocalAplicacao', 'CodLocalDestino', 'LocalDestino',
          'QtdAtual_RM', 'CodMaterial', 'DesMaterial',
          'CategoriaMaterial', 'SegmentoMaterial', 'Qtde_RM',
          'Solicitante_RM', 'CodCentroCusto_RM', 'DesCentroCusto_RM',
          'Fabricante_RM', 'Equivalancia_RM', 'RMGerouRC', 'Numero_RC',
          'SITUACAO_RC', 'Fase_RC', 'Data_RC', 'DataAprovacao_RC',
          'DATAENTREGA_RC', 'Comprador_RC', 'NomeAssinatura_RC',
          'NomeComprador_RC', 'Numero_MC', 'Data_MC', 'Fase_MC',
          'Situacao_MC', 'DataLiberacao_MC', 'ResponsavelLiberacao_MC',
          'Numero_OC', 'IdItem_OC', 'NomeAssinatura_OC', 'Data_OC',
          'DataAssinatura_OC', 'IdUsuarioAssinatura_OC', 'CodFornecedor_OC',
          'Fornecedor_OC', 'DataEntrega_OC', 'NumNotaFiscal_OC',
          'DataNotaFiscal_OC', 'Comprador_Id', 'CodCompradorOC',
          'NomeCompradorOC', 'PrecoUnitario', 'QtdeAtendida', 'QtdeComprada',
          'ValorIpi', 'ValorDesconto', 'SITUACAO_OC', 'CodigoMotivo_OC',
          'DescricaoMotivo_OC', 'CodigoNatureza_OC', 'DescricaoNatureza_OC',
          'TotalaReceberItem_OC', 'SapValorImposto_OC', 'empresa_id',
          'VlrBruto_OC', 'VlrDesconto_OC', 'DataNF', 'ValorNF',
          'DescontoNF', 'PrecoCustoServAplicado', 'QtdOrcadaServAplicado',
          'Aprovacao_OC', 'CcustoId_OC', 'CCustoID_RC', 'CCustoID_RM',
          'DataUltimaAssinatura_OC', 'NumeroPedidoSap_OC',
          'Solicitante_OC', 'Urgente', 'DataPrevEntregaOC',
          'Projeto_RM', 'Projeto_RC', 'Projeto_OC',
          'Status_OC', 'Projeto_ItemRM', 'Projeto_ItemRC',
          'IdCotacao']

df1 = df[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024

# df1.show()

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
