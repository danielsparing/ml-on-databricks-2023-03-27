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

dbfs_path = "/tmp/word2vec-get-started/insuranceqa/questions"
!head /dbfs{dbfs_path}/train.questions.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ###TODOs:
# MAGIC 
# MAGIC - use spark.read.csv to read in the different `*.questions.txt` files. See the output of the above `head` call to investigate what the separator character is and whether the files have headers.
# MAGIC    - Documentation <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option" target="_blank">here</a> (select the Python version)
# MAGIC - clean the `question_en` column so that it becomes all lowercase and multiple consecutive spaces become a single space.
# MAGIC    - hint: [withColumn](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html), [Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html)

# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace, col

def ingest_data(
  path,
  database,
  output_table
):

  spark.sql(f"create database if not exists {database}")
  spark.sql(f"drop table if exists {database}.{output_table}")

  # TODO: use spark.read.csv to read in the different `*.questions.txt` files.
  df = spark.read.csv(
    path,
    sep = # FILL-IN
    header = # FILL-IN
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

  # TODO: clean the `question_en` column so that it becomes all lowercase and multiple consecutive spaces become a single space.
  df = # FILL-IN
  return df

def pipeline(path, database, output_table):
  df = ingest_data(path, database, output_table)
  df = clean(df)
  df.write.saveAsTable(f"{database}.{output_table}")

pipeline(f"{dbfs_path}/train.questions.txt", "insuranceqa", "train")
pipeline(f"{dbfs_path}/test.questions.txt", "insuranceqa", "test")
pipeline(f"{dbfs_path}/valid.questions.txt", "insuranceqa", "valid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO's:
# MAGIC - use SQL cells (instead of Python cells) to view the first few rows of each of the three newly loaded tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- FILL-IN

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Discuss possibilities around mapping topics to answers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- FILL-IN

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- FILL-IN
