# Databricks notebook source
# MAGIC %md
# MAGIC ##### 11. Set up access to the Azure Data Lake Gen2 "Silver" container using an access key
# MAGIC ##### 12. Define the base path to read parquet files from the "silver" container

# COMMAND ----------

storage_account_name = "dbrickstorage01"
container_name = "silver"

access_key = dbutils.secrets.get(scope = "Dbricks_scope", key = "jetspace-access-key")

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  access_key)

# COMMAND ----------

basepath = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/hospital/"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 13. Function to load a Parquet file and create a temporary Spark SQL view
# MAGIC ##### 14. Load all relevant parquet files and create temporary views for SQL queries
# MAGIC

# COMMAND ----------

def load_parquet_view(path, view_name):
  df = spark.read.parquet(path)
  df.createOrReplaceTempView(view_name)
  print(f"Created view {view_name} ")
  return df

# COMMAND ----------

patients_df = load_parquet_view(basepath + "patients/", "patients")
doctors_df = load_parquet_view(basepath + "doctors/", "doctors")
billing_df = load_parquet_view(basepath + "billing/", "billing")
appointments_df = load_parquet_view(basepath + "appointments/", "appointments")
treatments_df = load_parquet_view(basepath + "treatments/", "treatments")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 15. Create temporary views for SQL queries

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW processed_view
# MAGIC AS
# MAGIC SELECT date_format(t.treatment_date, 'yyyy-MM') As Treatment_Date, concat(d.first_name, ' ', d.last_name) As Doctor_Name, d.hospital_branch, d.specialization, t.treatment_type, concat(p.first_name, ' ', p.last_name) As Patient_Name, count(DISTINCT a.appointment_id) As Total_Appointments, b.amount As Total_Bill, b.payment_status As Payment_Status, b.bill_date As Bill_Date
# MAGIC
# MAGIC FROM appointments a 
# MAGIC JOIN doctors d
# MAGIC ON a.doctor_id = d.doctor_id
# MAGIC JOIN treatments t
# MAGIC ON a.appointment_id = t.appointment_id
# MAGIC JOIN billing b
# MAGIC ON b.treatment_id = t.treatment_id
# MAGIC JOIN patients p
# MAGIC ON a.patient_id = p.patient_id
# MAGIC GROUP BY Treatment_Date, Doctor_Name, hospital_branch, specialization, treatment_type, Patient_Name, amount, payment_status, b.bill_date;
# MAGIC
# MAGIC SELECT * FROM processed_view

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 16. Set up access to the Azure Data Lake Gen2 "gold" container using a SAS token
# MAGIC ##### 17. Write the final DataFrame to the gold container in CSV format (overwrite if it exists)

# COMMAND ----------

storage_account_name = "dbrickstorage01"
container_name = "gold"
sas_token = dbutils.secrets.get(scope = "Dbricks_scope", key = "jetspace-access-key")

spark.conf.set(
  f"fs.azure.sas.{container_name}.{storage_account_name}.dfs.core.windows.net",
  sas_token)

# COMMAND ----------

gold_output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/final_table/hospital_export_csv"

# COMMAND ----------

final_df = spark.sql("SELECT * FROM processed_view")

# COMMAND ----------

final_df.write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(gold_output_path)