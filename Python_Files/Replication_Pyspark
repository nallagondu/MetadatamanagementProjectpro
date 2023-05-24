# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text('list1', "['customer']")
input=dbutils.widgets.get("list1")

# COMMAND ----------

l1=eval(input)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

files_list_dict={}
for i in dbutils.fs.ls('dbfs:/databricks-datasets/tpch/delta-001/'):
    if i.size ==0:
        files_list_dict[i.name[:-1]]=i.path

# COMMAND ----------

files_list_dict

# COMMAND ----------

path_list={}

# COMMAND ----------

for i in l1:
    if i in files_list_dict:
        path_list[i]=files_list_dict[i]


# COMMAND ----------

path_list

# COMMAND ----------

for k,v in path_list.items():
    print(k,v)
    try:
        dbutils.fs.ls('/mnt/Deltalake/replication_folder_delta_tables/'+k)
        is_replicated=True

    except:
        is_replicated=False
    if is_replicated:
        deltaTable = DeltaTable.forPath(spark, '/mnt/Deltalake/replication_folder_delta_tables/'+k)
        df1=spark.read.load(v)
        cond='target.C_custkey = updates.C_custkey'
        deltaTable.alias('target').merge(df1.alias('updates'),cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:

        df1=spark.read.load(v)
        df1.repartition(1).write.save("/mnt/Deltalake/replication_folder_delta_tables/"+str(k))
    
    


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Deltalake/replication_folder_delta_tables/
