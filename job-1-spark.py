#ler varios arquivos csv do dbfs com spark
# lendo todos os arquivos .csv do diretorio bigdata

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#setup da aplicação Spark

spark = SparkSession.builder.appName("job-1-spark")\
    .config("spark.sql.warehouse.dir", abspath('spark-warehouse'))\
    .config("fs.s3a.endpoint", "http://10.10.100.76:9000")\
    .config("fs.s3a.access.key","minioadmin")\
    .config("fs.s3a.secret.key","minioadmin")\
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("fs.s3a.path.style.access", "True")\
    .getOrCreate()

# definindo o metodo de logging da aplicação use INFO somente para DEV [INFO,ERROR]
spark.sparkContext.setLogLevel("ERROR")

# lendo os dados do Data Lake
df = spark.read.format("csv")\
     .option("header", "True")\
     .option("inferSchema", "True")\
     .csv("s3a://landing/*.csv")

# imprime os dados lidos na raw
print("\n Imprime os dados lidos na landing:")
print(df.show())

#imprime o schema do dataFrame
print("\n Imprime o schema do DataFrame lido na raw:")
print(df.printSchema())

#converte para o formato parquet
print("\n Escrevendo os dados lidos na raw para parquet na processing zone..")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://processing/df-parquet-file.parquet") 

#lendo os arquivos parquet
df_parquet = spark.read.format("parquet")\
    .load("s3a://processing/df-parquet-file.parquet")

#imprime os dados lidos em parquet da processing zone
print("\nImprime os dados lidos em parquet da processing zone..")
print(df_parquet.show())

#cria uma view para trabalhar em sql
df_parquet.createOrReplaceTempView("view_of_parquet")

#processa os dados conforme regra de negocio
df_result = spark.sql("SELECT BNF_CODE as Bnf_code \
                       ,SUM(ACT_COST) as Soma_Act_cost \
                       ,SUM(QUANTITY) as Soma_quantity \
                       ,SUM(ITEMS) as Soma_items \
                       ,AVG(ACT_COST) as Media_act_cost \
                       FROM view_of_parquet \
                       GROUP BY bnf_code" )  

#imprime o resultado do df criado
print("\n ========= Imprime o resutaldo do dataframe processado =========\n")
print(df_result.show())

#converte para formato parquet
print("\n Escrevendo os dados processados na Curated Zone...")

#convrte os dados processados para parquet e escreve na curated zone
df_result.write.format("parquet")\
         .mode("overwrite")\
         .save("s3a://curated/df_result_file.parquet")

#para a aplicação
spark.stop()