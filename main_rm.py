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
table = "vw_90_requisicao_material"
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
    
colums = ['Id','AplicacaoRequisicao_Id','NumRM','Data','DataPrevEntrega','STATUS',
          'StatusRel','LocalDestino_Id','CodDst','DscDst','LocalOrigem_Id',
          'CodOri','DscOri','localaplic_id','CodLocAplic','DscLocAplic',
          'MotivoRequisicao_id','CodMtv','DscMtv','Solicitante','SolicitanteCodigo',
          'SolicitanteDepartamento','SolicitanteEmail','SolicitanteTelefone',
          'SolicitanteRamal','SolicitanteCelular','Solicitante_Id','Equipamento_id',
          'CodEqp','DescricaoEquipamento','EspecieEquipamento','MarcaEquipamento',
          'ChassiEquipamento','NumeroSerieEquipamento','ModeloEquipamento','AnoFabricacaoEqp',
          'EquipApelido','Servico_id','CodPla','CodSrv','DscSrv','CodSubEmp','NomeSubEmp',
          'Emitido','Urgente','RazaoSocial','SolicitacaoServico','DataAssinatura','DataInclusao',
          'IdUsuInc','Empresa_Id','IdUsuAssinatura','Usuario_Id','LoginUsuInc','Fase',
          'CentroCustoId','RequisicaoMaterialEspelho_ID','NumeroOsSispeq','Parcial',
          'CentroCustoApropriacao_Id','CodigoCentroCusto','DescricaoCentroCusto',
          'TipoLogradouroEntrega','LogradouroEntrega','NumeroEntrega','CepEntrega',
          'ComplementoEntrega','BairroEntrega','UnidadeFederacaoEntrega','CidadeEntrega',
          'ContatoEntrega','Telefone1Entrega','Telefone2Entrega','EmailEntrega',
          'CpfCnpjEntrega','InscricaoEstadualEntrega','RazaoSocialEntrega','Projeto'
          ]

df1 = df[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024
# df1.show()

df1 = df1.filter((df1['Data'] > '2023-12-31') & (df1['IdUsuInc'] != '3342338'))

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
