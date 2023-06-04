# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text('user_input', "[{'source_type':'dbfs_delta_Table','table_name':'customer'}]")
input=dbutils.widgets.get("user_input")
#dbutils.widgets.removeAll()

# COMMAND ----------

delta_files_list_dict={}
for i in dbutils.fs.ls('dbfs:/databricks-datasets/tpch/delta-001/'):
    if i.size ==0:
        delta_files_list_dict[i.name[:-1]]=i.path

# COMMAND ----------

dbutils.fs.ls('/mnt/Deltalake/replication_folder_delta_tables/'+'customer')

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Deltalake"))

# COMMAND ----------

def delta_file_replication(tab_name,delta_files_list_dict):
    delta_tables_list={}
    try:
        print(dbutils.fs.ls('/mnt/replication/replication_folder_delta_tables/'+tab_name))
        full_load=False
    except:
        full_load=True
    if tab_name in delta_files_list_dict and not full_load:
            print("Merge completed sucessfully")
            deltaTable = DeltaTable.forPath(spark, '/mnt/replication/replication_folder_delta_tables/'+tab_name)
            df1=spark.read.load(delta_files_list_dict[tab_name])
            with open('/Workspace/Repos/rohith@azuredezyre.onmicrosoft.com/Metadatamanagement_Projectpro/Metadatamanagement/SourceDefinitionFiles/Supplier.json', 'r') as f:
  data = json.load(f)
            cond='target.C_custkey = updates.C_custkey'
            deltaTable.alias('target').merge(df1.alias('updates'),cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
           
    elif tab_name in delta_files_list_dict and  full_load:
        print("Full load completed")
        df1=spark.read.load(delta_files_list_dict[tab_name])
        df1.repartition(1).write.save("/mnt/replication/replication_folder_delta_tables/"+tab_name)
        

            

    else:
        print("No table ",tab_name," found with the path mentioned, Please recheck the list mentioned")








# COMMAND ----------

delta_files_list_dict

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt")

# COMMAND ----------

dbutils.fs.ls("/mnt/Deltalake")

# COMMAND ----------

for i in eval(input):
    print(i)
    if i["source_type"]=="dbfs_delta_Table":
        delta_file_replication(i["table_name"],delta_files_list_dict)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Deltalake/replication_folder_delta_tables/