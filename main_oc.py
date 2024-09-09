from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
from os.path import join, dirname
from dotenv import load_dotenv

# /usr/lib/jvm/java-19-openjdk-amd64

# set Java home
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-19-openjdk-amd64"
os.environ['SPARK_LOCAL_HOSTNAME'] = "172.30.45.213"

config_path = join(dirname(__file__), 'config/')
dotenv_path = join(dirname(__file__), 'config', '.env')
load_dotenv(dotenv_path)

HOST_SQLSRV = os.getenv('HOST_SQLSRV')
USER_SQLSRV = os.getenv('USER_SQLSRV')
PASS_SQLSRV = os.getenv('PASS_SQLSRV')
DB_SQLSRV = os.getenv('DB_SQLSRV')
DB_SQUEMA = os.getenv('DB_SQUEMA')

conf = SparkConf() \
    .setAppName("Analise OC") \
    .setMaster("local[2]") \
    .set("spark.driver.extraClassPath", "~/dev_prod/indicadores_supply/.venv/lib64/python3.10/site-packages/pyspark/*") \
    .set("spark.driver.extraClassPath", "/home/celso/dev_prod/indicadores_supply/_driver/sqljdbc_12.6/ptb/jars/*")
    # .set('spark.ui.showConsoleProgress', 'true')


sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)

print(spark.version)

table = "vw_90_ordem_compra"

jdbc_url = f'jdbc:sqlserver://{HOST_SQLSRV};database={DB_SQLSRV};encrypt=true;trustServerCertificate=true;'

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"{DB_SQUEMA}.{table}") \
    .option("user", USER_SQLSRV) \
    .option("password", PASS_SQLSRV) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    
colums = ['id','aplicacaorequisicao_id','mapacoleta_id', 
          'NumOC', 'data', 'datapreventrega', 'STATUS',
          'MotivoBaixa_Id', 'MtvCnc', 'DescricaoMotivoBaixa', 'origem',
          'CondicaoPagamento_Id', 'CodCondPg', 'DescricaoCondicaoPagamento',
          'DescricaoCompletaPagamento', 'tipocondicao', 'vlrtotbruto',
          'vlrtotdescitem', 'vlrtotipiitem', 'perdesconto', 'vlrdesconto',
          'vlrfrete', 'vlrfreteforanota', 'vlrseguro', 'outrosvlr',
          'controlereenviosap', 'numeropedido', 'numeropedidosap',
          'numerosegmentopedidosap', 'CodMtvCpa', 'DescricaoMotivoCompraOC',
          'CodCompr', 'NomeCompr', 'SeqNeg', 'fornecedormc_id', 'dataevolucao',
          'vlrtotservico', 'fornecedorprincipal_id', 'aprovacao',
          'CodDst', 'DscDst', 'CodOri', 'DscOri', 'CodLocAplic',
          'DscLocAplic', 'CodMtv', 'Solicitante',
          'SolicitanteDepartamento', 'Equipamento_Id',
          'CodEqp', 'Servico_id', 'CodSrv', 'CodFor',
          'emitido', 'NumMpa', 'DataMapa', 'urgente',
          'NomeFornecedor', 'CpfCnpjFornecedor',
          'solicitacaoservico', 'dataassinatura',
          'datainclusao', 'IdUsuInc', 'LoginUsuInc',
          'Fase', 'TipoMtv', 'DescricaoMotivoCompra',
          'DescricaoServico', 'DescricaoEquipamento',
          'EspecieEquipamento', 'ChassiEquipamento',
          'NumeroSerieEquipamento', 'MarcaEquipamento',
          'ModeloEquipamento', 'AnoFabricacaoEqp',
          'EquipApelido', 'NomeUsuarioAssinatura',
          'DescricaoMotivoRequisicao',
          'SituacaoTitulo', 'situacaointegracaosap',
          'situacaoalteracao', 'informoupercentualdesconto',
          'TotalReceber', 'TotalOC', 'LogradouroEntrega',
          'NumeroEntrega', 'ComplementoEntrega',
          'BairroEntrega', 'CepEntrega',
          'UnidadeFederacaoEntrega', 'CidadeEntrega',
          'RazaoSocialEntrega', 'Projeto', 'Conferido'
          ]

df1 = df[colums]

df1 = df1.filter((df1['data'] > '2023-12-31') & (df1['IdUsuInc'] != '3342338'))

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024

# df1.show()

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
