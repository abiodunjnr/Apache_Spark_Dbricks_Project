# Databricks notebook source
Dbricks_key = dbutils.secrets.get(scope = 'Dbricks_scope', key = 'jetspace-access-key')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %pip install kagglehub 

# COMMAND ----------

import os

os.environ['KAGGLE_USERNAME'] = dbutils.secrets.get(scope='kaggle_scope', key='kaggle_username')
os.environ['KAGGLE_KEY'] = dbutils.secrets.get(scope='kaggle_scope', key='kaggle_key')

print("Kaggle environment variables set.")

# COMMAND ----------

import kagglehub
from kagglehub import KaggleDatasetAdapter

def load_hospital_dataset():
    files = [
        "patients.csv",
        "doctors.csv",
        "appointments.csv",
        "treatments.csv",
        "billing.csv"
    ]
    
    dataframes = {}
    for file_name in files:
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            "kanakbaghel/hospital-management-dataset",
            file_name
        )
        dataframes[file_name] = df
        print(f"Loaded {file_name}, shape: {df.shape}")
    return dataframes

# Load once
dataframes = load_hospital_dataset()


# COMMAND ----------

def save_all_dfs_as_parquet(dataframes, base_path="/mnt/parquet/hospital"):
    parquet_paths = {}
    for filename, df in dataframes.items():
        table_name = filename.replace(".csv", "")
        spark_df = spark.createDataFrame(df)
        parquet_path = f"{base_path}/{table_name}"
        spark_df.write.format("parquet").mode("overwrite").save(parquet_path)
        print(f"Saved {table_name} at {parquet_path}")
        parquet_paths[table_name] = parquet_path
    return parquet_paths


# COMMAND ----------

dbutils.fs.unmount("/mnt/silver")

# COMMAND ----------

storage_account_name = "dbrickstorage01"
container_name = "silver"
mount_point = "/mnt/silver"
storage_account_access_key = dbutils.secrets.get(scope = 'Dbricks_scope', key = 'jetspace-access-key')

configs = {
  f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key
}

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = mount_point,
  extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt"))



# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession


dataframes = load_hospital_dataset()


spark = SparkSession.builder.getOrCreate()

for filename, pandas_df in dataframes.items():
    spark_df = spark.createDataFrame(pandas_df)
    
    # Use filename without extension as folder name
    folder_name = filename.replace(".csv", "")
    save_path = f"/mnt/silver/hospital/{folder_name}"
    
    # Write as parquet format, overwrite if exists
    spark_df.write.format("parquet").mode("overwrite").save(save_path)
    
    print(f"Saved {filename} to {save_path}")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/silver/hospital"))

# COMMAND ----------

def copy_all_to_blob(parquet_paths, blob_base_path="/mnt/blobstorage/hospital"):
    for table_name, parquet_path in parquet_paths.items():
        blob_path = f"{blob_base_path}/{table_name}"
        dbutils.fs.cp(parquet_path, blob_path, recurse=True)
        print(f"Copied {table_name} from {parquet_path} to {blob_path}")


# COMMAND ----------

spark.read.format("parquet").load("/mnt/silver/hospital/patients")