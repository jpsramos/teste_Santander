
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
import re

class SantanderAccessLog:
    def __init__(self, log_file_path, table_name):
        self.log_file_path = log_file_path
        self.table_name = table_name
        self.spark = SparkSession.builder.appName("Santander Access Log").getOrCreate()
        self.logs_df = None

    def load_logs(self):
        try:
            log_pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\d+)'
            
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

            logs_rdd = self.spark.sparkContext.textFile(self.log_file_path)
            parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)

            schema = StructType([
                StructField("ip", StringType(), True),
                StructField("client_identd", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("datetime", StringType(), True),
                StructField("request", StringType(), True),
                StructField("status", IntegerType(), True),
                StructField("size", IntegerType(), True),
            ])

            self.logs_df = self.spark.createDataFrame(parsed_logs_rdd, schema=schema)
            self.logs_df = self.logs_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), "dd/MMM/yyyy:HH:mm:ss Z"))
            print("Logs carregados com sucesso.")
        except Exception as e:
            print(f"Erro ao carregar os logs: {e}")

    def write_to_delta_table(self):
        try:
            self.logs_df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)
            print(f"Tabela Delta '{self.table_name}' criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela Delta '{self.table_name}': {e}")

    def exercise_1(self):
        try:
            top_ips = self.logs_df.groupBy("ip").count().orderBy(F.desc("count")).limit(10)
            print("Exercício 1: Top 10 IPs com mais acessos.")
            top_ips.show(truncate=False)
        except Exception as e:
            print(f"Erro no exercício 1: {e}")

    def exercise_2(self):
        try:
            df = self.logs_df.withColumn("endpoint", F.split(F.col("request"), " ").getItem(1))
            filtered_df = df.filter(~F.lower(F.col("endpoint")).rlike(
                r'\.(php|css|js|png|jpg|jpeg|gif|ico|svg|woff|ttf|eot|otf|map|json|xml|txt|zip|gz|tar|rar|7z)$'
            ))
            top_endpoints = filtered_df.groupBy("endpoint").count().orderBy(F.desc("count")).limit(6)
            print("Exercício 2: Top 6 endpoints mais acessados.")
            top_endpoints.show(truncate=False)
        except Exception as e:
            print(f"Erro no exercício 2: {e}")

    def exercise_3(self):
        try:
            distinct_ips = self.logs_df.select("ip").distinct().count()
            print(f"Exercício 3: Quantidade de Client IPs distintos: {distinct_ips}")
        except Exception as e:
            print(f"Erro no exercício 3: {e}")

    def exercise_4(self):
        try:
            distinct_days = self.logs_df.withColumn("date", F.to_date(F.col("datetime"))).select("date").distinct().count()
            print(f"Exercício 4: Quantidade de dias distintos representados no arquivo: {distinct_days}")
        except Exception as e:
            print(f"Erro no exercício 4: {e}")

    def exercise_5(self):
        try:
            results = self.logs_df.agg(
                F.sum("size").alias("total_volume"),
                F.max("size").alias("max_volume"),
                F.min("size").alias("min_volume"),
                F.avg("size").alias("avg_volume")
            ).collect()[0]
            print("Exercício 5: Análise de volumes de dados retornados.")
            print(f"Volume total de dados retornado: {results['total_volume']} bytes")
            print(f"Maior volume de dados em uma única resposta: {results['max_volume']} bytes")
            print(f"Menor volume de dados em uma única resposta: {results['min_volume']} bytes")
            print(f"Volume médio de dados retornado: {results['avg_volume']:.2f} bytes")
        except Exception as e:
            print(f"Erro no exercício 5: {e}")

    def exercise_6(self):
        try:
            df = self.logs_df.withColumn("day_of_week", F.date_format(F.col("datetime"), "EEEE"))
            client_errors_df = df.filter((F.col("status") >= 400) & (F.col("status") < 500))
            errors_by_day = client_errors_df.groupBy("day_of_week").count().orderBy(F.desc("count"))
            print("Exercício 6: Dia da semana com o maior número de erros do tipo 'HTTP Client Error'.")
            errors_by_day.show(truncate=False)
        except Exception as e:
            print(f"Erro no exercício 6: {e}")

# Exemplo de uso
log_file_path = "/FileStore/tables/access_log-5.txt"
table_name = "access_log"

santander_log = SantanderAccessLog(log_file_path, table_name)
santander_log.load_logs()
santander_log.write_to_delta_table()
santander_log.exercise_1()
santander_log.exercise_2()
santander_log.exercise_3()
santander_log.exercise_4()
santander_log.exercise_5()
santander_log.exercise_6()
