# batching application running locally
# import libraries
import pyspark
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
    spark = pyspark.sql.SparkSession.builder.appName("batch-silver-py")\
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

    # informacoes de parametros da configuracao
    print(SparkConf().getAll())

    # lendo dados dos usuarios na bronze em formato parquet
    df_users = spark.read.parquet(path_datalake + 'bronze/*.parquet')

    # schema do df_users
    df_users.printSchema()

    # consultando dados do df_users
    df_users.show()

    # criando engine sql para o df_users
    df_users.createOrReplaceTempView("users")

    df_result = spark.sql(
        """
        SELECT u.dt_update
             , u.email
             , u.endereco
             , u.dt_nascimento
             , CAST(floor(datediff(now(),u.dt_nascimento)/365.25) as INTEGER) as idade
             , CASE 
                 WHEN floor(datediff(now(),u.dt_nascimento)/365.25) > 65 then 'Idoso'
                 WHEN floor(datediff(now(),u.dt_nascimento)/365.25) BETWEEN 18 AND 65 then 'Adulto'
                 WHEN floor(datediff(now(),u.dt_nascimento)/365.25) BETWEEN 13 AND 17 then 'Adolescente'
                 ELSE 'Criança'
               END as grupo_idade
             , u.nome
             , u.profissao
             , u.sexo
             , u.telefone
          FROM users u
        """
    )

    # consultando dados do df_results
    df_result.show()

    # schema do df_results
    df_result.printSchema()

    # gravando os dados na camada Silver em formato Delta
    df_result.write.mode('overwrite')\
        .format("delta")\
        .partitionBy('grupo_idade')\
        .option("overwriteSchema", "true")\
        .save(path_datalake + 'silver')

    # encerra sessao spark
    spark.stop()
