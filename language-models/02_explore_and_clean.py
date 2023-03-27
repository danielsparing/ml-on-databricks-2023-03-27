# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Insurance Q&A Intent Classification with Databricks & Hugging Face
# MAGIC ### Data Exploration
# MAGIC 
# MAGIC <hr />
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/rafaelvp-db/dbx-insurance-qa-hugging-face/master/img/header.png" width="800px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Looking at the amount of intents

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from insuranceqa.train

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Distribution of question lengths

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC - Use the length function to create a new column "length" as the length of column `question_en`, and run [summary statistics](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.summary.html) on this column, output to the new dataframe `df_summary`.

# COMMAND ----------

from pyspark.sql.functions import col, length

df_summary = (
  spark.sql("select * from insuranceqa.train")
    .withColumn("length", # FILL-IN
    # FILL-IN
    # FILL-IN
)

display(df_summary)

# COMMAND ----------

# DBTITLE 1,Initial Insights
# MAGIC %md
# MAGIC 
# MAGIC * There's a really long tail in terms of questions lengths
# MAGIC * As a rule of thumb, questions that are this long are usually not that useful / insightful; we'll consider these as anomalies and drop them
# MAGIC * This will be useful when it comes to tokenizing these inputs, as we'll save some valuable GPU memory that would otherwise be wasted with zeros due to padding (truncating these samples could make the model confused)

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC given the earlier statistics you calculated, filter the dataset to lengths less than the 75th percentile.

# COMMAND ----------

def remove_outliers(df):

  df = (
    df
      .withColumn("length", length(col("question_en")))
      # TODO: Limit size to approx. 75th quantile
      .filter( # FILL-IN
      .drop("length")
  )
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC Use the below flag to specify if you have a GPU available on your current cluster or not. The cell below will sample the training dataset if no GPU is available. Warning: if you re-execute this cell multiple times, it will keep reducing your dataset! (if this happens, just re-run the previous notebook.)

# COMMAND ----------

USE_GPU = True

# COMMAND ----------

for table in ["insuranceqa.train", "insuranceqa.test", "insuranceqa.valid"]:

  df = spark.sql(f"select * from {table}")
  df = remove_outliers(df)
  if not USE_GPU and table == "insuranceqa.train":
    df = df.sample(fraction=0.3)
  df.write.saveAsTable(name = table, mode = "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC TODO:
# MAGIC count the distinct English topics in the training dataset. Note: there should be 12 of them, if you get less, the above sampling might be too aggressive.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: count the distinct English topics in the training dataset.
# MAGIC select -- FILL-IN

# COMMAND ----------


