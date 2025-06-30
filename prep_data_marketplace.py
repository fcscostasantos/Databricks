# Databricks notebook source
# MAGIC %md
# MAGIC <img src ='https://i.imgur.com/HRhd2Y0.png'>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Importa Dados

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists mkt_olist;

# COMMAND ----------

spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_orders_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_orders'
)

# COMMAND ----------

spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_order_payments_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_pagamentos'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Manipulação dos dados

# COMMAND ----------

# Read Orders Table
df_orders = spark.table(
  'mkt_olist.mkt_olist_orders'
)

# Read Payment Table
df_payment = spark.table(
  'mkt_olist.mkt_olist_pagamentos'
)



# COMMAND ----------

# DBTITLE 1,Total de Vendas por tipo pagamento
df_payment.groupBy(
  'payment_type' 
).agg(
  avg('payment_value').alias('avg_payment')
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select payment_type
# MAGIC ,avg(payment_value) as avg_payment
# MAGIC from mkt_olist.mkt_olist_pagamentos
# MAGIC group by all
# MAGIC --group by 1
# MAGIC --group by payment_type

# COMMAND ----------

spark.sql(
  """
    select payment_type
    ,avg(payment_value) as avg_payment
    from mkt_olist.mkt_olist_pagamentos
    group by all
  """
).display()

# COMMAND ----------

# DBTITLE 1,Faturamento Mes a Mes Analítico
df_orders.join(
  df_payment, ['order_id'], 'left'
).filter(
  col('order_status')=='invoiced'
).withColumn(
  'Mes', lpad(month(to_date(col('order_approved_at'))),2,'0')
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*
# MAGIC ,b.*
# MAGIC ,lpad(month(to_date(a.order_approved_at)),2,'0') as Mes
# MAGIC from mkt_olist.mkt_olist_orders as a
# MAGIC left join mkt_olist.mkt_olist_pagamentos as b
# MAGIC on a.order_id = b.order_id
# MAGIC where a.order_status = "invoiced"

# COMMAND ----------

spark.sql(
  """
  select a.*
  ,b.*
  ,lpad(month(to_date(a.order_approved_at)),2,'0') as Mes
  from mkt_olist.mkt_olist_orders as a
  left join mkt_olist.mkt_olist_pagamentos as b
  on a.order_id = b.order_id
  where a.order_status = "invoiced"
  """
).display()

# COMMAND ----------

# DBTITLE 1,Qual foi o faturamento mês a mês?
df_orders_vs_payment = df_orders.join(
  df_payment, ['order_id'], 'left'
).filter(
  col('order_status')=='invoiced'
).withColumn(
  'Mes', lpad(month(to_date(col('order_approved_at'))),2,'0')
).groupBy(
  'Mes'
).agg(
  sum('payment_value').cast('decimal(32,2)').alias('Faturamento')
).orderBy(
  col('Mes').desc()
)

df_orders_vs_payment.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select mes
# MAGIC ,cast(sum(payment_value) as decimal(32,2)) as Faturamento
# MAGIC from(
# MAGIC select a.*
# MAGIC   ,b.*
# MAGIC   ,lpad(month(to_date(a.order_approved_at)),2,'0') as Mes
# MAGIC   from mkt_olist.mkt_olist_orders as a
# MAGIC   left join mkt_olist.mkt_olist_pagamentos as b
# MAGIC   on a.order_id = b.order_id
# MAGIC   where a.order_status = "invoiced"
# MAGIC )
# MAGIC group by all
# MAGIC order by mes desc

# COMMAND ----------

spark.sql(
  """
  select mes
  ,cast(sum(payment_value) as decimal(32,2)) as Faturamento
  from(
  select a.*
    ,b.*
    ,lpad(month(to_date(a.order_approved_at)),2,'0') as Mes
    from mkt_olist.mkt_olist_orders as a
    left join mkt_olist.mkt_olist_pagamentos as b
    on a.order_id = b.order_id
    where a.order_status = "invoiced"
  )
  group by all
  order by mes desc
  """
).display()