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


def get_spark():

    conf = SparkConf() \
        .setAppName("Analise OC") \
        .setMaster("local[2]") \
        .set("spark.driver.extraClassPath", "~/dev_prod/indicadores_supply/.venv/lib64/python3.10/site-packages/pyspark/*") \
        .set("spark.driver.extraClassPath", "/home/celso/dev_prod/indicadores_supply/_driver/sqljdbc_12.6/ptb/jars/*")
    # .set('spark.ui.showConsoleProgress', 'true')

    sc = SparkContext.getOrCreate(conf=conf)

    return SparkSession(sc)


def get_spark_df(spark, table):

    jdbc_url = f'jdbc:sqlserver://{HOST_SQLSRV};database={DB_SQLSRV};encrypt=true;trustServerCertificate=true;'

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{DB_SQUEMA}.{table}") \
        .option("user", USER_SQLSRV) \
        .option("password", PASS_SQLSRV) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
        
    return df
