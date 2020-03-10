# Databricks notebook source
spark.conf.set("fs.azure.account.key.maadlsgen2.dfs.core.windows.net", dbutils.secrets.get(scope = "databricks-key-vault-secret-scopes", key = "adlsgen2-secret-key")) 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('abfss://myblobcontainer@databrickteststorage3.dfs.core.windows.net/dossier/Kansas_City_Monthly_Car_Auction.csv')

print("Count of auction cars: ", df.count())

from pyspark.sql.functions import *

df2 = df.select(column("Vehicle ID"), col("Lot #").alias("lot_num"), col("Tow Reference ").alias("tow_reference"), col("Year").alias("year"), col("Make").alias("make"),col("Model").alias("model"),col("VIN").alias("vin"),col("Mileage").alias("mileage"),col("Reason").alias("reason"),col("K").alias("k"),col("Comments").alias("comments"),) 
df2.printSchema()
print(df2.count())

# COMMAND ----------

writeConfig = { "Endpoint" : "https://databricks-tests-cosmosdb.documents.azure.com:443/", "Masterkey" : "nIBXGLwuq8dnurCX6I1cpUEVn79r2EuHWolLWP2sOKbyVJrwBrUe48q6dq1xkIsNHApA5QNApb9BNqsktdTEXQ==", "Database" : "database1", "Collection" : "cars", "Upsert" : "true" }

# COMMAND ----------

readConfig = { "Endpoint" : "<COSMOSDB_URI>", "Masterkey" : "https://databricks-tests-cosmosdb.documents.azure.com:443/", "Database" : "database1", "preferredRegions" : "West Europe", "Collection" : "cars", "SamplingRatio" : "1.0", "schema_samplesize" : "1000", "query_pagesize" : "2147483647", "query_custom" : "SELECT * FROM cars" }

cars = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load() 
cars.count()

display(cars.orderBy(col("vehicleID")))