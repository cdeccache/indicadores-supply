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
table = "vw_90_itemRC"
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

colums = ['ItemRM_Id','ItemMC_Id','ProcessoCompra_Id','Id','RMGerouRC','Fase_RC','MotivoRequisicao_Id',
          'CodMtv','DescricaoMotivoRequisicao','Comprador_RC','Numero_MC','Data_MC','Fase_MC','Situacao_MC',
          'DataLiberacao_MC','ResponsavelLiberacao_MC','EquipApelido','SITUACAO_RC','STATUS',
          'ItemProcessoCompra_ID','AplicacaoRequisicao_id','UnidadeStq','sequencia','DescricaoIpc',
          'OperadorConv','FatorConv','Marca','Fase','ItemServico','tipo','SAPRegraDistribuicao1_Id',
          'SAPCodRegraDistribuicao1_RC','SAPDscRegraDistribuicao1_RC','SAPRegraDistribuicao2_Id',
          'SAPCodRegraDistribuicao2_RC','SAPDscRegraDistribuicao2_RC','SAPRegraDistribuicao3_Id',
          'SAPCodRegraDistribuicao3_RC','SAPDscRegraDistribuicao3_RC','SAPRegraDistribuicao4_Id',
          'SAPCodRegraDistribuicao4_RC','SAPDscRegraDistribuicao4_RC','SAPRegraDistribuicao5_Id',
          'SAPCodRegraDistribuicao5_RC','SAPDscRegraDistribuicao5_RC','Numero','Material_Id','CodigoMaterial',
          'DescricaoMaterial','ReferenciaMaterial','OperadorConvInsumo','FatorConvInsumo','MaterialID',
          'descricao','DescricaoCategoria','CodigoCategoria','UniPro','DescCompletaUnidadeMaterial',
          'FabricanteMaterial_Id','CodigoEquivalencia','Fabricante_Id','CodigoFabricante','DescricaoFabricante',
          'IdItemDocumentoDevolucao','LocalDestino_Id','CodDst','DscDst','LocalOrigem_Id','CodOri','DscOri',
          'empresa_id','QtdRequisitada','QtdComprada','QtdAtendida','QtdCanceladaRC','servico_id','CodigoPlanilha',
          'CodigoServico','DescricaoServico','Natureza_Id','CodigoNatureza','DescricaoNatureza',
          'CodigoSubEmpreiteiro','MotivoBaixa_Id','CodigoMotivoBaixa','DescricaoMtvBaixa','CentroCustoApropriacao_Id',
          'SubempreiteiroContrato_Id','NumeroRM','SequenciaRM','NumeroMC','SequenciaMC','Equipamento_Id',
          'CodigoEquipamento','DescricaoEquipamento','ApelidoEquipamento','EspecieEquipamento','MarcaEquipamento',
          'ModeloEquipamento','AnoFabricacaoEqp','ChassiEquipamento','NumeroSerieEquipamento',
          'CentroCustoApropriacaoEquipamento_id','IdCentroCustoEquipamento','CodigoCentroCustoEquipamento',
          'DescricaoCentroCustoEquipamento','DescricaoSubEmpreiteiro','CodigoContratoSubEmp','PrecoOrcadoInsumo',
          'PrecoOrcadoEstoque','CentroCusto_Id','CodigoCentroCusto','DescricaoCentroCusto','TipoCentroCusto',
          'Projeto','SAPUtilizacao_Id','SAPUtilizacaoCodigo','SAPUtilizacaoDescricao','SAPUnidadeGrupo_Id',
          'CodigoUnidadeSAP','QuantidadeBase','QuantidadeUnidade','DataPrevisaoEntrega','DataPrevPresServico']

df1 = df[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024
# df1.show()

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
