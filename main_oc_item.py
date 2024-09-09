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
table = "vw_90_itemOC"
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

colums = ['itemrc_id', 'processocompra_id', 'ItemProcessoCompra_Id', 'CodFornecedor_OC',
          'Fornecedor_OC', 'EquipApelido', 'Material_OC_Id', 'CodMaterial_OC', 'CategoriaMat_OC',
          'SegmentoMat_OC', 'DesMaterial_OC', 'CodigoEquivalencia', 'TipoFrete_OC', 'Seq_OC',
          'SAPRegraDistribuicao1_Id', 'SAPCodRegraDistribuicao1_OC', 'SAPDscRegraDistribuicao1_OC',
          'SAPRegraDistribuicao2_Id', 'SAPCodRegraDistribuicao2_OC', 'SAPDscRegraDistribuicao2_OC',
          'SAPRegraDistribuicao3_Id', 'SAPCodRegraDistribuicao3_OC', 'SAPDscRegraDistribuicao3_OC',
          'SAPRegraDistribuicao4_Id', 'SAPCodRegraDistribuicao4_OC', 'SAPDscRegraDistribuicao4_OC',
          'SAPRegraDistribuicao5_Id', 'SAPCodRegraDistribuicao5_OC', 'SAPDscRegraDistribuicao5_OC',
          'PrecoUnitario', 'QtdeAtendida', 'QtdeComprada', 'ValorIpi', 'ValorDesconto', 'SITUACAO_OC',
          'Subempreiteiro_OC_Id', 'CodSubempreiteiro_OC', 'Subempreiteiro_OC', 'ValorNF', 'DescontoNF',
          'VlrBruto_OC', 'VlrDesconto_OC', 'cotacaomc_id', 'qtdcomprada', 'qtdatendida', 'qtdcancelada',
          'pcounitario', 'perdesconto', 'vlrdesconto', 'peripi', 'vlripi', 'ipiincluso', 'pericms',
          'vlricms', 'prazoentrega', 'procedencia', 'vlrunitunid1', 'vlricmsst', 'aplicacaorequisicao_id',
          'sequencia', 'unidadestq', 'operadorconv', 'fatorconv', 'marca', 'fase', 'StatusItem',
          'CodigoStatusItemOC', 'itemservico', 'tipo', 'NumeroOC', 'material_id', 'CodigoMaterial',
          'categoria_id', 'CodigoCategoria', 'DescricaoCategoria', 'DescricaoUnidadeMaterial',
          'DescCompletaUnidadeMaterial', 'Fabricante_Id', 'CodigoFabricante', 'DescricaoFabricante',
          'MotivoRequisicao_Id', 'CodigoMotivoRequisicao', 'DescricaoMotivoRequisicao', 'Servico_Id',
          'CodigoPlanilha', 'CodigoServico', 'DescricaoServico', 'PrecoCustoServAplicado',
          'QtdOrcadaServAplicado', 'Equipamento_Id', 'CodigoEquipamento', 'DescricaoEquipamento',
          'MarcaEquipamento', 'ModeloEquipamento', 'AnoFabricacaoEqp', 'EspecieEquipamento',
          'ChassiEquipamento', 'NumeroSerieEquipamento', 'Natureza_Id', 'CodigoNatureza',
          'DescricaoNatureza', 'ContratoSubempreiteiro', 'ContratoSubempreiteiro_Id', 'centrocusto_id',
          'MotivoBaixa_ID', 'CodigoMotivoBaixa', 'DescricaoMotivoBaixa', 'centrocustoapropriacao_id',
          'CodDst', 'DscDst', 'CodOri', 'DscOri', 'LocAplic_Id', 'CodLocAplic', 'DscLocAplic',
          'OperadorConvInsumo', 'FatorConvInsumo', 'PrecoOrcadoInsumo', 'PrecoOrcadoEstoque', 'TipoItem',
          'NomeSubEmp', 'NumeroRC', 'SequenciaItemRC', 'CodOriRC', 'SequenciaMapa', 'materialcritico',
          'CentroCustoApr_Id', 'CodigoCentroCusto', 'DescricaoCentroCusto', 'TipoCentroCusto', 'TotalaReceber',
          'CodigoInsumo', 'DataMapa', 'DataEscrituracaoNotaFiscal', 'DataInclusaoNotaFiscal', 'NumeroNotaFiscal',
          'ValorDifal', 'SapValorImposto', 'Projeto', 'SAPUtilizacao_Id', 'SAPUtilizacaoCodigo',
          'SAPUtilizacaoDescricao', 'SAPUnidadeGrupo_Id', 'CodigoUnidadeSAP', 'QuantidadeBase',
          'QuantidadeUnidade', 'SapCodigoImposto_Id', 'SAPImpostoCodigo', 'DataPrevisaoEntrega',
          'DataPrevPresServico'
          ]

df1 = df[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024

# df1.show()

df1.write.csv(f'_dados/{table}', mode='overwrite', header=True)
