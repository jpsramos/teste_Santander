# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
#from pyspark.sql.functions import to_timestamp, col
import re

# Criar uma SparkSession
spark = SparkSession.builder \
    .appName("Web Server Access Log") \
    .getOrCreate()

# Caminho para o arquivo de log
log_file_path = "/FileStore/tables/access_log-5.txt"  # Substitua pelo caminho do seu arquivo

# Ler o arquivo de log como RDD
logs_rdd = spark.sparkContext.textFile(log_file_path)

# Regex ajustado para o formato do log fornecido
log_pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\d+)'

# Função para processar cada linha do log
def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        return (
            match.group(1),  # ip
            match.group(2),  # client_identd
            match.group(3),  # user_id
            match.group(4),  # datetime
            match.group(5),  # request
            int(match.group(6)),  # status
            int(match.group(7)),  # size
        )
    else:
        return None

# Processar o RDD para extrair os campos
parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)

# Definir o esquema explicitamente
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("client_identd", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("request", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("size", IntegerType(), True),
])

# Converter o RDD para DataFrame usando o esquema
logs_df = spark.createDataFrame(parsed_logs_rdd, schema=schema)
# Converter campo para timestamp
logs_df = logs_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), "dd/MMM/yyyy:HH:mm:ss Z"))

# Mostrar os dados
logs_df.show(truncate=True)

def write_to_delta_table(df, table_name, path=None):
    """
    Escreve um DataFrame no formato Delta em um caminho especificado e registra como uma tabela Delta.

    :param df: DataFrame do PySpark a ser salvo.
    :param table_name: Nome da tabela Delta a ser criada.
    :param path: Caminho onde os dados Delta serão armazenados (opcional).
    """
    try:
        if path:
            # Escrever o DataFrame no formato Delta em um caminho específico
            df.write.format("delta").mode("overwrite").option("path", path).saveAsTable(table_name)
            print(f"Tabela Delta '{table_name}' salva com sucesso no caminho '{path}'.")
        else:
            # Escrever o DataFrame no formato Delta sem especificar o caminho
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"Tabela Delta '{table_name}' salva com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar a tabela Delta '{table_name}': {e}")

# Exemplo de uso
delta_table_name = "access_log"
#delta_path = "/mnt/delta-tables/access_log"  # Substitua pelo caminho desejado

# Chamar a função para salvar o DataFrame como Delta
write_to_delta_table(logs_df, delta_table_name)

# COMMAND ----------

# 1
from pyspark.sql import DataFrame

def get_top_n_ips(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Retorna os top N IPs com mais acessos em um DataFrame.

    :param df: DataFrame do PySpark contendo os dados de log.
    :param n: Número de IPs a serem retornados (padrão é 10).
    :return: DataFrame contendo os top N IPs com mais acessos.
    """
    # Agrupar por IP e contar os acessos
    ip_access_count = df.groupBy("ip").count()

    # Ordenar em ordem decrescente pelo número de acessos e selecionar os N primeiros
    top_n_ips = ip_access_count.orderBy(F.desc("count")).limit(n)

    print(f"Exercício 1 executado...")

    return top_n_ips

# Exemplo de uso
top_10_ips = get_top_n_ips(logs_df, n=10)
top_10_ips.show(truncate=False)

# COMMAND ----------

# 2
def get_top_n_endpoints(df: DataFrame, n: int = 6) -> DataFrame:
    """
    Retorna os top N endpoints mais acessados, excluindo aqueles que representam arquivos comuns.
    
    :param df: DataFrame PySpark contendo os dados de log.
    :param n: Número de endpoints a serem retornados (padrão é 6).
    :return: DataFrame contendo os top N endpoints mais acessados.
    """
    # Extrair o endpoint do campo "request"
    df = df.withColumn("endpoint", F.split(F.col("request"), " ").getItem(1))
    
    # Filtrar endpoints que não representam arquivos (excluindo extensões comuns)
    filtered_df = df.filter(~F.lower(F.col("endpoint")).rlike(
        r'\.(php|css|js|png|jpg|jpeg|gif|ico|svg|woff|ttf|eot|otf|map|json|xml|txt|zip|gz|tar|rar|7z)$'
    ))
    
    # Agrupar por endpoint e contar os acessos
    endpoint_access_count = filtered_df.groupBy("endpoint").count()
    
    # Ordenar em ordem decrescente pelo número de acessos e selecionar os N primeiros
    top_n_endpoints = endpoint_access_count.orderBy(F.desc("count")).limit(n)
    
    print(f"Exercício 2 executado...")

    return top_n_endpoints

# Exemplo de uso
top_6_endpoints = get_top_n_endpoints(logs_df, n=6)
top_6_endpoints.show(truncate=False)

# COMMAND ----------

# 3
def count_distinct_ips(df: DataFrame) -> int:
    """
    Conta a quantidade de Client IPs distintos em um DataFrame PySpark.

    :param df: DataFrame do PySpark contendo os dados de log.
    :return: Quantidade de Client IPs distintos.
    """
    # Contar a quantidade de Client IPs distintos
    distinct_ips_count = df.select("ip").distinct().count()
    return print(f"Quantidade de Client IPs distintos: {distinct_ips_count}")

# Exemplo de uso
distinct_ips_count = count_distinct_ips(logs_df)


# COMMAND ----------

#4

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def count_distinct_days(df: DataFrame, datetime_col: str) -> int:
    """
    Conta a quantidade de dias distintos representados em um DataFrame PySpark.

    :param df: DataFrame PySpark contendo os dados de log.
    :param datetime_col: Nome da coluna que contém os valores de data e hora.
    :return: Quantidade de dias distintos.
    """

    # Extrair apenas a data (sem o horário)
    df = df.withColumn("date", F.to_date(F.col(datetime_col)))
    
    # Contar os dias distintos
    distinct_days_count = df.select("date").distinct().count()
    
    return print(f"Quantidade de dias distintos representados no arquivo: {distinct_days_count}")

# Exemplo de uso
distinct_days_count = count_distinct_days(logs_df, "datetime")


# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def analyze_response_volumes(df: DataFrame, datetime_col: str, size_col: str) -> list:
    """
    Realiza a análise de volumes de dados retornados em um DataFrame do PySpark.

    :param df: DataFrame do PySpark contendo os dados de log.
    :param datetime_col: Nome da coluna que contém os valores de data e hora.
    :param size_col: Nome da coluna que contém os tamanhos das respostas.
    :return: Uma lista de strings formatadas com os resultados da análise.
    """
    # Converter a coluna datetime para o formato de timestamp
    df = df.withColumn(datetime_col, F.to_timestamp(F.col(datetime_col), "dd/MMM/yyyy:HH:mm:ss Z"))

    # Realizar a análise
    results = df.agg(
        F.sum(size_col).alias("total_volume"),
        F.max(size_col).alias("max_volume"),
        F.min(size_col).alias("min_volume"),
        F.avg(size_col).alias("avg_volume")
    ).collect()[0]

    ## Retornar os resultados como um dicionário
    #return {
    #    "total_volume": results["total_volume"],
    #    "max_volume": results["max_volume"],
    #    "min_volume": results["min_volume"],
    #    "avg_volume": results["avg_volume"]
    #}

    # Retornar os resultados como uma lista de strings formatadas
    return [
        f"Volume total de dados retornado: {results['total_volume']} bytes",
        f"Maior volume de dados em uma única resposta: {results['max_volume']} bytes",
        f"Menor volume de dados em uma única resposta: {results['min_volume']} bytes",
        f"Volume médio de dados retornado: {results['avg_volume']:.2f} bytes"
    ]

# Exemplo de uso
analysis_results = analyze_response_volumes(logs_df, "datetime", "size")

# Exibir os resultados
for result in analysis_results:
    print(result)


## Exibir os resultados
#print(f"Volume total de dados retornado: {analysis_results['total_volume']} bytes")
#print(f"Maior volume de dados em uma única resposta: {analysis_results['max_volume']} bytes")
#print(f"Menor volume de dados em uma única resposta: {analysis_results['min_volume']} bytes")
#print(f"Volume médio de dados retornado: {analysis_results['avg_volume']:.2f} bytes")

# COMMAND ----------

#6

def analyze_http_client_errors_by_day(df: DataFrame, datetime_col: str, status_col: str) -> DataFrame:
    """
    Analisa os erros do tipo "HTTP Client Error" (status 400-499) em um DataFrame do PySpark
    e retorna o número de erros agrupados por dia da semana, ordenados em ordem decrescente.

    :param df: DataFrame PySpark contendo os dados de log.
    :param datetime_col: Nome da coluna que contém os valores de data e hora.
    :param status_col: Nome da coluna que contém os códigos de status HTTP.
    :return: DataFrame com os dias da semana e o número de erros, ordenados em ordem decrescente.
    """
    # Adicionar uma coluna para o dia da semana
    df = df.withColumn("day_of_week", F.date_format(F.col(datetime_col), "EEEE"))

    # Filtrar apenas os erros do tipo "HTTP Client Error" (status 400-499)
    client_errors_df = df.filter((F.col(status_col) >= 400) & (F.col(status_col) < 500))

    # Contar o número de erros por dia da semana
    errors_by_day = client_errors_df.groupBy("day_of_week").count()

    # Ordenar os resultados pelo número de erros em ordem decrescente
    errors_by_day_sorted = errors_by_day.orderBy(F.desc("count"))

    return errors_by_day_sorted

# Exemplo de uso
errors_by_day_sorted = analyze_http_client_errors_by_day(logs_df, "datetime", "status")

# Mostrar os resultados
errors_by_day_sorted.show()

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Criar uma SparkSession
spark = SparkSession.builder \
    .appName("Delta Table Analysis") \
    .getOrCreate()

# Nome da tabela Delta registrada no metastore
delta_table_name = "access_log"  # Substitua pelo nome da sua tabela Delta

# Ler a tabela Delta pelo nome
logs_df = spark.table(delta_table_name)

logs_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from access_log limit 10

# COMMAND ----------

from pyspark.sql import SparkSession

# Criar uma SparkSession
spark = SparkSession.builder.appName("Consultar Tabelas").getOrCreate()


# Consultar dados de uma tabela específica
dados = spark.sql("SELECT * FROM access_log LIMIT 10")
dados.show()

# COMMAND ----------

# Carregar a tabela diretamente
tabela_df = spark.table("access_log")

# Mostrar os primeiros registros
tabela_df.show()
