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
    
    # lendo dados dos usuarios na bronze em formato parquet
    df_users = spark.read.format("delta").load(path_datalake + "silver/users")

    # schema do df_users
    df_users.printSchema()  

    # criando engine sql para o df_users
    df_users.createOrReplaceTempView("users")

    df_result = spark.sql(
        """
        SELECT TO_DATE(u.dt_update) dt_base
             , u.grupo_idade
             , u.sexo
             , ROUND(AVG(u.idade),2) idade_media
             , COUNT(1) total_users
          FROM users u
      GROUP BY to_date(u.dt_update)
             , u.grupo_idade
             , u.sexo
        """
    )
    
    # gravando os dados na camada Gold em formato Delta
    df_result.write.mode('overwrite')\
        .format("delta")\
        .option("overwriteSchema", "true")\
        .partitionBy('dt_base', 'grupo_idade')\
        .save(path_datalake + 'gold/users')

    # encerra sessao spark
    spark.stop()
