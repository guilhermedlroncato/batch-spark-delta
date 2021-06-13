# batching application running locally
# import libraries
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import to_date, to_timestamp
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F


if __name__ == '__main__':
    
    # path data-lake local
    path_datalake = '/home/guilherme/Python/Projetos/batch-spark-delta/data-lake/'

    # inicio sessão spark
    spark = SparkSession \
        .builder \
        .appName("batch-bronze-py") \
        .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
        .enableHiveSupport() \
        .getOrCreate()

    # informacoes de parametros da configuracao
    print(SparkConf().getAll())

    # lendo dados dos usuarios na landing
    df_users = spark.read \
        .format('json') \
        .option('inferSchema', 'true') \
        .option('header', 'true') \
        .json(path_datalake + 'landing/users/*.json')

    # schema do df_users
    df_users.printSchema()

    # consultando dados do df_users
    df_users.show()

    # criando engie sql para o df_users
    df_users.createOrReplaceTempView("users")

    df_result = spark.sql(
        """
        SELECT to_timestamp(u.dt_update) as dt_update
             , u.email
             , u.endereco
             , to_date(u.nascimento) as dt_nascimento
             , u.nome
             , u.profissao
             , u.sexo
             , u.telefone
          FROM users u
        """
    )

    # verificando schema do df_result
    df_result.printSchema()

    # consultando o resultado
    df_result.show()

    # salvando os dados em formato parquet na camada bronze
    df_result.write.mode("overwrite")\
        .format("parquet")\
        .save(path_datalake + 'bronze/users')

    # encerra sessao spark
    spark.stop()