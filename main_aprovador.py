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
table = "aprovador"
user = 'sa'
password = 'PasSw0rd'

jdbc_url = f'jdbc:sqlserver://{host};database={database};encrypt=true;trustServerCertificate=true;'


query = """ SELECT
                pa.Id,
                pa.ProcessoCompra_Id,
                pa.SituacaoAprovacao AS SituacaoAprovacao,
                pa.DataInclusao AS DataInclusao,
                pa.dataaprovacao AS DataAprovacao,
                vtu.CodigoFuncionario AS Matricula,
                vtu.NomeFuncionario AS Nome,
                ta.Descricao AS TipoAprovador
            FROM
                dbo.processocompra_aprovador pa
            INNER JOIN Seel.dbo.tipo_aprovador ta ON
                pa.TipoAprovador_Id = ta.Id
            INNER JOIN vw_90_TableUsuario vtu ON
                pa.IdUsuario = vtu.Id
"""

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", query) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

df.write.csv(f'_dados/{table}', mode='overwrite', header=True)
