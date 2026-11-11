import os
import sys
import urllib.request
from pyspark.sql import SparkSession

# 1. Привязываем воркеров к виртуальному окружению Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. Инициализируем чистый Спарк (JAR-драйверы больше не нужны!)
spark = SparkSession.builder \
    .appName("ClickhouseToSparkDirectHTTP") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# 3. Формируем URL для выгрузки сырого CSV с заголовками напрямую из ClickHouse
clickhouse_url = (
    "http://localhost:8123/"
    "?user=default"
    "&password="
    "&database=clean_dwh"
    "&query=SELECT+*+FROM+customers+FORMAT+CSVWithNames"
)

print("Запрашиваем данные из ClickHouse напрямую по HTTP...")

# 4. Скачиваем CSV-поток в память как список строк
with urllib.request.urlopen(clickhouse_url) as response:
    # Декодируем байты в текст и разбиваем на строки, убирая пустые
    csv_lines = [line.decode('utf-8').strip() for line in response.readlines() if line]

# Разделяем заголовок и сами данные
header = csv_lines[0].split(',')
data_lines = csv_lines[1:]

# 5. Превращаем текстовые строки в RDD, а затем в Spark DataFrame
# Разделяем каждую строку по запятой, формируя записи
rdd = spark.sparkContext.parallelize(data_lines).map(lambda line: line.split(','))

# Создаем DataFrame, используя имена колонок из CSV-заголовка ClickHouse
df_customers = spark.createDataFrame(rdd, schema=header)

# 6. Проверяем результат
df_customers.show(5)
print(f"Успешно загружено строк напрямую через HTTP-пакеты: {df_customers.count()}")
