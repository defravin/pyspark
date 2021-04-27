# Databricks notebook source
import requests
scrutiniUrl= "https://raw.githubusercontent.com/tenaris/scala-spark-workshop/master/data/referendum2016/scrutini2016.csv"
dbutils.fs.put("scrutini2016.csv", requests.get(scrutiniUrl).text, True)

#Read from file system the CSV file just downloaded

#Turn on the schema inferation from the column type
#Take the columns names from first row
scrutiniDS = spark.read.option("inferSchema","true").option("header","true").csv("/scrutini2016.csv")
#  .option("inferSchema", "true") \
#  .option("header","true") \
#  .csv("scrutini2016.csv")

#Verify that the schema corresponds to expected one and column type is correct
scrutiniDS.printSchema()


# COMMAND ----------

# Verify that the dataset contains all Italian regions.
regioniItalianeDS = scrutiniDS.select("DESCREGIONE").distinct()
assert regioniItalianeDS.count() == 20

#Compute the number of voters (Votanti) per region and order the DataFrame according to it
scrutiniDS.groupBy("DESCREGIONE").sum("VOTANTI").orderBy("sum(VOTANTI)",ascending= False).show(10)

# COMMAND ----------

# Compute the percentage of positive votes (NUMVOTISI) per region.
scrutiniPerRegioneDS = scrutiniDS.groupBy("DESCREGIONE").sum("VOTANTI","NUMVOTISI").orderBy("sum(VOTANTI)",ascending=False)
scrutiniPerRegioneDS \
  .withColumn("PERCENTUALESI", scrutiniPerRegioneDS["sum(NUMVOTISI)"] / scrutiniPerRegioneDS["sum(VOTANTI)"]) \
  .orderBy("PERCENTUALESI", ascending=False) \
  .show(10,truncate=False)
