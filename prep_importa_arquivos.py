# Databricks notebook source
# DBTITLE 1,Importa Bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import re

# COMMAND ----------

# DBTITLE 1,Cria o Banco de Dados
# MAGIC %sql
# MAGIC create database if not exists marketplace

# COMMAND ----------

# DBTITLE 1,Importa Arquivo CSV
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/data.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'marketplace.table_data_marketplace'
)

# COMMAND ----------

# DBTITLE 1,Seleciona o mostra os dados depois de importados
# MAGIC %sql
# MAGIC select * from marketplace.table_data_marketplace

# COMMAND ----------

# DBTITLE 1,Mosta os dados da tabela utilizando pyspark
spark.table(
  'marketplace.table_data_marketplace'
).display()

# COMMAND ----------

# DBTITLE 1,Agrupa e soma os a quantidade e preço unitário
spark.table(
  'marketplace.table_data_marketplace'
).groupBy(
  'Country'
).agg(
  sum('Quantity').alias('sum_quantity')
  ,sum('UnitPrice').cast('decimal(20,2)').alias('sum_UnitPrice')
).orderBy(
  col('sum_UnitPrice').desc()
).display()

# COMMAND ----------

# DBTITLE 1,Efetua a mesma soma mencionado na célula acima mas utilizando sql
# MAGIC %sql
# MAGIC
# MAGIC select Country
# MAGIC ,sum(Quantity) as sum_quantity
# MAGIC ,sum(UnitPrice) as sum_UnitPrice
# MAGIC from marketplace.table_data_marketplace
# MAGIC group by all
# MAGIC order by sum_UnitPrice desc

# COMMAND ----------

# DBTITLE 1,Salva o arquivo no DBFS em formato csv
spark.table(
  'marketplace.table_data_marketplace'
).groupBy(
  'Country'
).agg(
  sum('Quantity').alias('sum_quantity')
  ,sum('UnitPrice').cast('decimal(20,2)').alias('sum_UnitPrice')
).orderBy(
  col('sum_UnitPrice').desc()
).write.option(
  'header', 'true'
).mode(
  'overwrite'
).csv(
  '/FileStore/dados_marketplace_canal_youtube'
)

# COMMAND ----------

# DBTITLE 1,Gera o link de url para Download do arquivo(Obs.: não está funcionando)
def generate_final_url(filename_output=None):
  user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split('@')[0]
  directory = f'dbfs:/FileStore/tables/{filename_output}'
  files = dbutils.fs.ls(directory)
  for file in files:
    filename = file.name
    if filename.endswith(".csv"):
      final_url = f'https://community.cloud.databricks.com/files/{filename_output}/{filename}?o=316934541180447'
      return final_url

generate_final_url('dados_marketplace_canal_youtube')

# COMMAND ----------

# DBTITLE 1,Código para importar mais de um arquivo por vez
csv_dir_path = "/FileStore/tables"  # ajuste para seu caminho real

arquivos_desejados = [
"olist_customers_dataset.csv",
"olist_order_items_dataset.csv",
"olist_orders_dataset.csv",
]
caminhos_csv = [f"{csv_dir_path}/{nome}" for nome in arquivos_desejados]

for file_path in caminhos_csv:
    match = re.search(r'/([^/]+)\.csv$', file_path)
    if match:
        table_name = match.group(1)
    else:
        print(f"Erro ao extrair nome da tabela: {file_path}")
        continue

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Tabela '{table_name}' criada a partir de '{file_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_customers_dataset

# COMMAND ----------

spark.table(
  'olist_order_items_dataset'
).display()

# COMMAND ----------

spark.sql(
  """
    select * from olist_order_items_dataset
  """
).display()