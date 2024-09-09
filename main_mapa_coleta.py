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
table = "vw_90_mapacoleta"
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

colums = ['Id', 'Empresa_Id', 'IdUsuario', 'NumeroMC', 'DataMC', 'Fase', 'FaseMC',
          'ObservacaoMC', 'NomeUsuarioMC', 'Comprador_Id', 'NomeComprador', 'TelefoneComprador',
          'EmailComprador', 'CodigoComprador', 'NegociacaoId', 'SequenciaNegociacao', 'DataAberturaNegociacao',
          'DataAutorizacaoMC', 'UsuarioAutorizouCompraMC', 'Material_Id', 'CodigoMaterial', 'DescricaoMaterial',
          'UniPro', 'CotacaoId', 'ItemMC_Id', 'SequenciaMC', 'DataCotacao', 'QuantidadeCanceladaMC',
          'QuantidadeCompradaMC', 'QuantidadeRequisitadaMC', 'SaldoQuantidadeMC', 'PrecoUnitarioMC',
          'NumeroRC', 'DataRC', 'SolicitanteRC', 'NumeroRM', 'DataRM', 'Equipamento_Id', 'AnoFabricacaoEqp',
          'EquipApelido', 'ChassiEquipamento', 'CodEqp', 'MarcaEquipamento', 'ModeloEquipamento', 'DescricaoEquipamento',
          'NumeroSerieEquipamento', 'TipoEquipamento', 'EspecieEquipamento', 'CodigoFornecedor', 'NomeFornecedor',
          'RazaoSocialFornecedor', 'PercentualDescontoItem', 'VlrIPI', 'VlrIcmsSt', 'ValorCotacao', 'TotalItem',
          'VlrDesconto', 'VlrFrete', 'VlrSeguro', 'VlrFreteForaNota', 'DataEntrega', 'OutrosVlr', 'CondicaoPagamento',
          'PrecoUltimaCompra', 'CpfCnpjFornecedor', 'CodOri', 'DscOri', 'CodDst', 'DscDst']

df1 = df[colums]


# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024
# df1.show()

df1 = df1.filter(df1['DataMC'] > '2023-12-31')

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
