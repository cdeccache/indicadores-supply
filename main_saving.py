from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
from os.path import join, dirname
from dotenv import load_dotenv
import pandas as pd

from functions import get_spark, get_spark_df


spark = get_spark()

print(spark.version)

table_mc = "vw_90_mapacoleta"

table_oc = "vw_90_ordem_compra"

table_pc = "processo_compra"


df1 = get_spark_df(spark=spark, table=table_mc)

colums = ['Id', 'Empresa_Id', 'IdUsuario', 'NumeroMC', 'DataMC', 'Fase', 'FaseMC',
          'NomeUsuarioMC', 'NegociacaoId', 'SequenciaNegociacao', 'DataAberturaNegociacao',
          'DataAutorizacaoMC', 'UsuarioAutorizouCompraMC', 'Material_Id', 'CodigoMaterial', 'DescricaoMaterial',
          'UniPro', 'CotacaoId', 'ItemMC_Id', 'SequenciaMC', 'DataCotacao', 'QuantidadeCanceladaMC',
          'QuantidadeCompradaMC', 'QuantidadeRequisitadaMC', 'SaldoQuantidadeMC', 'PrecoUnitarioMC',
          'NumeroRC', 'DataRC', 'SolicitanteRC', 'NumeroRM', 'DataRM', 'CodigoFornecedor', 'NomeFornecedor',
          'RazaoSocialFornecedor', 'ValorCotacao', 'TotalItem', 'DataEntrega', 'CondicaoPagamento',
          'PrecoUltimaCompra', 'CodOri', 'DscOri', 'CodDst', 'DscDst']

df_mc = df1[colums]


df2 = get_spark_df(spark=spark, table=table_oc)

colums = ['NumOC', 'NumMpa', 'data', 'vlrtotbruto']

df_oc = df2[colums]

# COLOCAR FILTRO para retirar usuário 90TI/dhiego e dados a partir de 01/01/2024

# df1 = df1.filter((df1['DataMC'] > '2023-12-31')
#                  & (df1['NumeroMC'] == 2075)
#                  & (df1['NegociacaoId'] == 246012428)
#                  )
# df1 = df1.na.fill({'age': 50, 'name': 'unknown'})

df_mc = df_mc.where(""" DataMC > '2023-12-31'
                    AND NomeFornecedor NOT LIKE 'MC -%'
                    AND NomeFornecedor NOT LIKE 'SEEL%' """)  # AND NumeroMC = 2075

# Agrupar por NumeroMC, NegociacaoId e Forn
cols = ['Id', 'DataAberturaNegociacao', 'NumeroMC',
        'NegociacaoId', 'CodigoFornecedor', 'NomeFornecedor']
df_mc = df_mc.groupBy(cols).sum('TotalItem')

# preencher código fornecedor vazio
df_mc = df_mc.na.fill({'CodigoFornecedor': 'F00000'})

df_mc = df_mc.where("`sum(TotalItem)` > 0").orderBy(
    "`NumeroMC`", "`NegociacaoId`")

df_mc.select('Id', 'DataAberturaNegociacao', 'NumeroMC', 'NegociacaoId',
             'CodigoFornecedor', 'NomeFornecedor', '`sum(TotalItem)`').show(5)

df_mc.createOrReplaceTempView('dados_mc')

dados_mc_new = spark.sql(""" SELECT DISTINCT
                                Id, NumeroMC, NegociacaoId,
                                MIN(`sum(TotalItem)`) AS min_total
                            FROM dados_mc
                            WHERE NumeroMC IS NOT NULL
                                AND DataAberturaNegociacao > '2024-04-01'
                                -- AND NumeroMC IN (976, 2135, 2075)
                            GROUP BY all
                     """).toPandas()

# cols = ['Id', 'NumeroMC', 'NegociacaoId',
#         'MIN(`sum(TotalItem)`)', 'NumOC', 'NumMpa', 'data', 'vlrtotbruto']

cols = ['NumMpa', 'data', 'sum(vlrtotbruto)', 'min_value']
cols1 = ['NumMpa', 'data']


dados = pd.DataFrame()

for index, row in dados_mc_new.iterrows():
    num_mapa = row['NumeroMC']
    min_total = row['min_total']
    
    df3 = pd.DataFrame(columns=cols)
    
    if num_mapa is not None:
        df_oc1 = df_oc.where(f"NumMpa = {num_mapa}")

        # df_oc1.show()

        df_oc2 = df_oc1.groupBy(cols1).sum('vlrtotbruto')

        # df_oc2.show()

        df4 = df_oc2.toPandas()

        for i, r in df4.iterrows():
            # if row_mapa is not None:
            df3.loc[index] = r
            df3['min_value'] = min_total

    dados = pd.concat([df3, dados], ignore_index=True)


df_mc.write.csv(f'_dados/saving', mode='overwrite', header=True, sep=';')

dados.to_csv('_dados/saving/dados_mc.csv', sep=';',
             header=['mapa', 'data', 'total_oc', 'valor_min'])
