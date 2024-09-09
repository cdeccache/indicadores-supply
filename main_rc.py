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
table = "vw_90_requisicao_compra"
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

colums = ['Id', 'CodigoLocalAplicacao', 'DescricaoLocalAplicacao', 'CodDst',
          'DscDst', 'CodOri', 'DscOri', 'NumRC', 'Data', 'DataPrevEntrega', 'STATUS', 'MotivoBaixa_Id',
          'MtvCnc', 'DescricaoMtvCnc', 'CodMtv', 'DescricaoMotivoCompra', 'Solicitante',
          'SolicitanteCodigo', 'SolicitanteDepartamento', 'Solicitante_Id', 'Emitido', 'NumMpa',
          'ListaNumMpa', 'Urgente', 'SolicitacaoServico', 'DataAssinatura', 'DataInclusao', 'NomeUsuarioAssinatura',
          'NomeUsuarioInclusao', 'Fase', 'CentroCustoId', 'Equipamento_Id', 'CodEqp', 'DescricaoEquipamento',
          'EspecieEquipamento', 'MarcaEquipamento', 'ModeloEquipamento', 'AnoFabricacaoEqp', 'EquipApelido',
          'ChassiEquipamento', 'NumeroSerieEquipamento', 'SubEmpreiteiro_Id', 'ContratoSubempreiteiro',
          'CodSubEmp', 'NomeSubEmp', 'CentroCustoApropriacao_Id', 'CodigoCentroCusto', 'DescricaoCentroCusto',
          'RazaoSocialEntrega', 'CodigoComprador', 'NomeComprador', 'Projeto']

df1 = df[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024
# df1.show()

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
