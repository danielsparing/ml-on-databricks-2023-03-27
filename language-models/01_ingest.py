# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Insurance Q&A Intent Classification with Databricks & Hugging Face
# MAGIC ### Data Ingestion
# MAGIC 
# MAGIC <hr />
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/rafaelvp-db/dbx-insurance-qa-hugging-face/master/img/header.png" width="800px"/>

# COMMAND ----------

!rm -rf /tmp/word2vec-get-started
!rm -rf /dbfs/tmp/word2vec-get-started
%cd /tmp
!git clone https://github.com/hailiang-wang/word2vec-get-started.git
!mv /tmp/word2vec-get-started/corpus /dbfs/tmp/word2vec-get-started

# COMMAND ----------

dbfs_path = "/tmp/word2vec-get-started/insuranceqa/questions"
!ls -alh /dbfs{dbfs_path}

# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace, col

def ingest_data(
  path,
  database,
  output_table
):

  spark.sql(f"create database if not exists {database}")
  spark.sql(f"drop table if exists {database}.{output_table}")

  df = spark.read.csv(
    path,
    sep = "\t",
    header = True
  )

  df = df.toDF(
    'id',
    'topic_en',
    'topic_jp',
    'question_en',
    'question_jp'
  )\
  .select("id", "topic_en", "question_en")

  return df

def clean(df):

  df = df.withColumn("question_en", regexp_replace(lower(col("question_en")), "  ", " "))
  return df

def pipeline(path, database, output_table):
  df = ingest_data(path, database, output_table)
  df = clean(df)
  df.write.saveAsTable(f"{database}.{output_table}")

pipeline(f"{dbfs_path}/train.questions.txt", "insuranceqa", "train")
pipeline(f"{dbfs_path}/test.questions.txt", "insuranceqa", "test")
pipeline(f"{dbfs_path}/valid.questions.txt", "insuranceqa", "valid")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from insuranceqa.train limit 20

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Discuss possibilities around mapping topics to answers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from insuranceqa.test limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from insuranceqa.valid limit 10
