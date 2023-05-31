// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md 
// MAGIC # Exercise #2 - Batch Ingestion
// MAGIC
// MAGIC In this exercise you will be ingesting three batches of orders, one for 2017, 2018 and 2019.
// MAGIC
// MAGIC As each batch is ingested, we are going to append it to a new Delta table, unifying all the datasets into one single dataset.
// MAGIC
// MAGIC Each year, different individuals and different standards were used resulting in datasets that vary slightly:
// MAGIC * In 2017 the backup was written as fixed-width text files
// MAGIC * In 2018 the backup was written a tab-separated text files
// MAGIC * In 2019 the backup was written as a "standard" comma-separted text files but the format of the column names was changed
// MAGIC
// MAGIC Our only goal here is to unify all the datasets while tracking the source of each record (ingested file name and ingested timestamp) should additional problems arise.
// MAGIC
// MAGIC Because we are only concerned with ingestion at this stage, the majority of the columns will be ingested as simple strings and in future exercises we will address this issue (and others) with various transformations.
// MAGIC
// MAGIC As you progress, several "reality checks" will be provided to you help ensure that you are on track - simply run the corresponding command after implementing the corresponding solution.
// MAGIC
// MAGIC This exercise is broken up into 3 steps:
// MAGIC * Exercise 2.A - Ingest Fixed-Width File
// MAGIC * Exercise 2.B - Ingest Tab-Separated File
// MAGIC * Exercise 2.C - Ingest Comma-Separated File

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #2</h2>
// MAGIC
// MAGIC To get started, we first need to configure your Registration ID and then run the setup notebook.

// COMMAND ----------

// MAGIC %md ### Setup - Registration ID
// MAGIC
// MAGIC In the next commmand, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
// MAGIC
// MAGIC For more information, see [Registration ID]($./Registration ID)

// COMMAND ----------

// MAGIC %python
// MAGIC registration_id = "3149411"

// COMMAND ----------

// MAGIC %md ### Setup - Run the exercise setup
// MAGIC
// MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

// COMMAND ----------

// MAGIC %run ./_includes/Setup-Exercise-02

// COMMAND ----------

reality_check_02_a()

// COMMAND ----------

// MAGIC %md Run the following cell to preview a list of the files you will be processing in this exercise.

// COMMAND ----------

// MAGIC %python
// MAGIC files = dbutils.fs.ls(f"{working_dir}/raw/orders/batch") # List all the files
// MAGIC display(files)                                           # Display the list of files

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.A - Ingest Fixed-Width File</h2>
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC 1. Use the variable **`batch_2017_path`**, and **`dbutils.fs.head`** to investigate the 2017 batch file, if needed.
// MAGIC 2. Configure a **`DataFrameReader`** to ingest the text file identified by **`batch_2017_path`** - this should provide one record per line, with a single column named **`value`**
// MAGIC 3. Using the information in **`fixed_width_column_defs`** (or the dictionary itself) use the **`value`** column to extract each new column of the appropriate length.<br/>
// MAGIC   * The dictionary's key is the column name
// MAGIC   * The first element in the dictionary's value is the starting position of that column's data
// MAGIC   * The second element in the dictionary's value is the length of that column's data
// MAGIC 4. Once you are done with the **`value`** column, remove it.
// MAGIC 5. For each new column created in step #3, remove any leading whitespace
// MAGIC   * The introduction of \[leading\] white space should be expected when extracting fixed-width values out of the **`value`** column.
// MAGIC 6. For each new column created in step #3, replace all empty strings with **`null`**.
// MAGIC   * After trimming white space, any column for which a value was not specified in the original dataset should result in an empty string.
// MAGIC 7. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from.
// MAGIC   * This should not be hard coded.
// MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
// MAGIC 8. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame.
// MAGIC   * This should not be hard coded.
// MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
// MAGIC 9. Write the corresponding **`DataFrame`** in the "delta" format to the location specified by **`batch_target_path`**
// MAGIC
// MAGIC **Special Notes:**
// MAGIC * It is possible to use the dictionary **`fixed_width_column_defs`** and programatically extract <br/>
// MAGIC   each column but, it is also perfectly OK to hard code this step and extract one column at a time.
// MAGIC * The **`SparkSession`** is already provided to you as an instance of **`spark`**.
// MAGIC * The classes/methods that you will need for this exercise include:
// MAGIC   * **`pyspark.sql.DataFrameReader`** to ingest data
// MAGIC   * **`pyspark.sql.DataFrameWriter`** to ingest data
// MAGIC   * **`pyspark.sql.Column`** to transform data
// MAGIC   * Various functions from the **`pyspark.sql.functions`** module
// MAGIC   * Various transformations and actions from **`pyspark.sql.DataFrame`**
// MAGIC * The following methods can be used to investigate and manipulate the Databricks File System (DBFS)
// MAGIC   * **`dbutils.fs.ls(..)`** for listing files
// MAGIC   * **`dbutils.fs.rm(..)`** for removing files
// MAGIC   * **`dbutils.fs.head(..)`** to view the first N bytes of a file
// MAGIC
// MAGIC **Additional Requirements:**
// MAGIC * The unified batch dataset must be written to disk in the "delta" format
// MAGIC * The schema for the unified batch dataset must be:
// MAGIC   * **`submitted_at`**:**`string`**
// MAGIC   * **`order_id`**:**`string`**
// MAGIC   * **`customer_id`**:**`string`**
// MAGIC   * **`sales_rep_id`**:**`string`**
// MAGIC   * **`sales_rep_ssn`**:**`string`**
// MAGIC   * **`sales_rep_first_name`**:**`string`**
// MAGIC   * **`sales_rep_last_name`**:**`string`**
// MAGIC   * **`sales_rep_address`**:**`string`**
// MAGIC   * **`sales_rep_city`**:**`string`**
// MAGIC   * **`sales_rep_state`**:**`string`**
// MAGIC   * **`sales_rep_zip`**:**`string`**
// MAGIC   * **`shipping_address_attention`**:**`string`**
// MAGIC   * **`shipping_address_address`**:**`string`**
// MAGIC   * **`shipping_address_city`**:**`string`**
// MAGIC   * **`shipping_address_state`**:**`string`**
// MAGIC   * **`shipping_address_zip`**:**`string`**
// MAGIC   * **`product_id`**:**`string`**
// MAGIC   * **`product_quantity`**:**`string`**
// MAGIC   * **`product_sold_price`**:**`string`**
// MAGIC   * **`ingest_file_name`**:**`string`**
// MAGIC   * **`ingested_at`**:**`timestamp`**

// COMMAND ----------

// MAGIC %md ### Fixed-Width Meta Data 
// MAGIC
// MAGIC The following dictionary is provided for reference and/or implementation<br/>
// MAGIC (depending on which strategy you choose to employ).
// MAGIC
// MAGIC Run the following cell to instantiate it.

// COMMAND ----------

// MAGIC %python
// MAGIC fixed_width_column_defs = {
// MAGIC   "submitted_at": (1, 15),
// MAGIC   "order_id": (16, 40),
// MAGIC   "customer_id": (56, 40),
// MAGIC   "sales_rep_id": (96, 40),
// MAGIC   "sales_rep_ssn": (136, 15),
// MAGIC   "sales_rep_first_name": (151, 15),
// MAGIC   "sales_rep_last_name": (166, 15),
// MAGIC   "sales_rep_address": (181, 40),
// MAGIC   "sales_rep_city": (221, 20),
// MAGIC   "sales_rep_state": (241, 2),
// MAGIC   "sales_rep_zip": (243, 5),
// MAGIC   "shipping_address_attention": (248, 30),
// MAGIC   "shipping_address_address": (278, 40),
// MAGIC   "shipping_address_city": (318, 20),
// MAGIC   "shipping_address_state": (338, 2),
// MAGIC   "shipping_address_zip": (340, 5),
// MAGIC   "product_id": (345, 40),
// MAGIC   "product_quantity": (385, 5),
// MAGIC   "product_sold_price": (390, 20)
// MAGIC }

// COMMAND ----------

// MAGIC %fs ls "dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/batch_orders_dirty.delta/"

// COMMAND ----------

val path = "dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/batch_orders_dirty.delta/"
dbutils.fs.ls(path).map(_.name)
.foreach((file: String) => dbutils.fs.rm(path + file, true))

// COMMAND ----------

// MAGIC %md ### Implement Exercise #2.A
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

import org.apache.spark.sql.functions._
val batch_2017_path="dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/raw/orders/batch/2017.txt"
val customerDf2017 = spark.read.format("text").option("inferSchema",true).load(batch_2017_path)
val newDF = customerDf2017
  .withColumn("submitted_at",trim(substring(col("value"), 1, 15)))
.withColumn("submitted_at",
       when(col("submitted_at")==="" ,null)
          .otherwise(col("submitted_at")))

  .withColumn("order_id",trim(substring(col("value"), 16, 40)))
.withColumn("order_id",
       when(col("order_id")==="" ,null)
          .otherwise(col("order_id")))

  .withColumn("customer_id",trim(substring(col("value"), 56, 40)))
.withColumn("customer_id",
       when(col("customer_id")==="" ,null)
          .otherwise(col("customer_id")))

  .withColumn("sales_rep_id",trim(substring(col("value"), 96, 40)))
.withColumn("sales_rep_id",
       when(col("sales_rep_id")==="" ,null)
          .otherwise(col("sales_rep_id")))

  .withColumn("sales_rep_ssn",trim(substring(col("value"), 136, 15)))
.withColumn("sales_rep_ssn",
       when(col("sales_rep_ssn")==="" ,null)
          .otherwise(col("sales_rep_ssn")))

  .withColumn("sales_rep_first_name",trim(substring(col("value"), 151, 15)))
.withColumn("sales_rep_first_name",
       when(col("sales_rep_first_name")==="" ,null)
          .otherwise(col("sales_rep_first_name")))

  .withColumn("sales_rep_last_name",trim(substring(col("value"), 166, 15)))
.withColumn("sales_rep_last_name",
       when(col("sales_rep_last_name")==="" ,null)
          .otherwise(col("sales_rep_last_name")))

  .withColumn("sales_rep_address",trim(substring(col("value"), 181, 40)))
.withColumn("sales_rep_address",
       when(col("sales_rep_address")==="" ,null)
          .otherwise(col("sales_rep_address")))

  .withColumn("sales_rep_city",trim(substring(col("value"), 221, 20)))
.withColumn("sales_rep_city",
       when(col("sales_rep_city")==="" ,null)
          .otherwise(col("sales_rep_city")))

  .withColumn("sales_rep_state",trim(substring(col("value"), 241, 2)))
.withColumn("sales_rep_state",
       when(col("sales_rep_state")==="" ,null)
          .otherwise(col("sales_rep_state")))

  .withColumn("sales_rep_zip",trim(substring(col("value"), 243, 5)))
.withColumn("sales_rep_zip",
       when(col("sales_rep_zip")==="" ,null)
          .otherwise(col("sales_rep_zip")))

  .withColumn("shipping_address_attention",trim(substring(col("value"), 248, 30)))
.withColumn("shipping_address_attention",
       when(col("shipping_address_attention")==="" ,null)
          .otherwise(col("shipping_address_attention")))

  .withColumn("shipping_address_address",trim(substring(col("value"), 278, 40)))
.withColumn("shipping_address_address",
       when(col("shipping_address_address")==="" ,null)
          .otherwise(col("shipping_address_address")))

  .withColumn("shipping_address_city",trim(substring(col("value"), 318, 20)))
.withColumn("shipping_address_city",
       when(col("shipping_address_city")==="" ,null)
          .otherwise(col("shipping_address_city")))

  .withColumn("shipping_address_state",trim(substring(col("value"), 338, 2)))
.withColumn("shipping_address_state",
       when(col("shipping_address_state")==="" ,null)
          .otherwise(col("shipping_address_state")))

  .withColumn("shipping_address_zip",trim(substring(col("value"), 340, 5)))
.withColumn("shipping_address_zip",
       when(col("shipping_address_zip")==="" ,null)
          .otherwise(col("shipping_address_zip")))

  .withColumn("product_id",trim(substring(col("value"), 345, 40)))
.withColumn("product_id",
       when(col("product_id")==="" ,null)
          .otherwise(col("product_id")))

  .withColumn("product_quantity",trim(substring(col("value"), 385, 5)))
.withColumn("product_quantity",
       when(col("product_quantity")==="" ,null)
          .otherwise(col("product_quantity")))

  .withColumn("product_sold_price",trim(substring(col("value"), 390, 20)))
.withColumn("product_sold_price",
       when(col("product_sold_price")==="" ,null)
          .otherwise(col("product_sold_price")))

  .withColumn("ingest_file_name",input_file_name)
  .withColumn("ingested_at",current_timestamp)
  .drop("value")


println(newDF.count)
display(newDF)
val batch_target_path = "dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/batch_orders_dirty.delta"
newDF.write
  .mode("overwrite")
  .format("delta")
  .save(batch_target_path)


// COMMAND ----------

// MAGIC %md ### Reality Check #2.A
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_02_a()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.B - Ingest Tab-Separted File</h2>
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC 1. Use the variable **`batch_2018_path`**, and **`dbutils.fs.head`** to investigate the 2018 batch file, if needed.
// MAGIC 2. Configure a **`DataFrameReader`** to ingest the tab-separated file identified by **`batch_2018_path`**
// MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
// MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
// MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created datasets specified by **`batch_target_path`**
// MAGIC
// MAGIC **Additional Requirements**
// MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**

// COMMAND ----------

// MAGIC %md ### Implement Exercise #2.b
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

/*import org.apache.spark.sql.functions._
val batch_2018_path="dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/raw/orders/batch/2018.csv"
val dfschema="submitted_at string,order_id string,customer_id string,sales_rep_id string,sales_rep_ssn string,sales_rep_first_name string,sales_rep_last_name string,sales_rep_address string,sales_rep_city string,sales_rep_state string,sales_rep_zip string,shipping_address_attention string,shipping_address_address string,shipping_address_city string,shipping_address_state string,shipping_address_zip string,product_id string,product_quantity string,product_sold_price string"
val fileReadOptions = Map("header" -> "true", "delimiter" -> "\t","inferSchema"->"true") //.option("delimiter", "\t")
val customerDf2018 = spark.read.format("csv").options(fileReadOptions).load(batch_2018_path)
.withColumn("ingest_file_name",input_file_name)
  .withColumn("ingested_at",current_timestamp)
.union(newDF)

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
/*val colArr= Array("submitted_at","order_id","customer_id", "sales_rep_id","sales_rep_ssn", "sales_rep_first_name",  "sales_rep_last_name","sales_rep_address",  "sales_rep_city",  "sales_rep_state",  "sales_rep_zip",  "shipping_address_attention",  "shipping_address_address",  "shipping_address_city",  "shipping_address_state",  "shipping_address_zip",  "product_id",  "product_quantity",  "product_sold_price","ingest_file_name","ingested_at")
 var i:Int=0
def sqlNullAdder(df: DataFrame,columnName:Array[String]):DataFrame={
  while(i< columnName.length){
 df.withColumn(columnName(i),
      when((col(columnName(i))) === "null", lit(null: String).cast(StringType)).otherwise(col(columnName(i))).as(columnName(i)))       
   i=i+1
  }
  df
}
val last2018DF=sqlNullAdder(customerDf2018,colArr) 
*/

    
private def setEmptyToNull(df: DataFrame): DataFrame = {
    val exprs = df.schema.map {f =>
      f.dataType match {
        case StringType => when((col(f.name)) === "null", lit(null: String).cast(StringType)).otherwise(col(f.name)).as(f.name)
        case _ => col(f.name)
      }
    }
  df.select(exprs: _*)
  }
val last2018DF = setEmptyToNull(customerDf2018)




display(last2018DF )
last2018DF 
.write
.format("delta")
.mode(SaveMode.Overwrite)
.option("path","dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/batch_orders_dirty.delta")
.save
customerDf2018.count
*/



import org.apache.spark.sql.functions.{trim,ltrim,rtrim,col}

import org.apache.spark.sql.functions.{col,when}

import org.apache.spark.sql._

import org.apache.spark.sql.functions.{input_file_name,current_timestamp}

import org.apache.spark.sql.types._

val batch_2018_path="dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/raw/orders/batch/2018.csv"

 

var customerDF2018 = spark.read.format("csv")

            .option("delimiter", "\t")

            .option("header",true)

            .load(batch_2018_path)

            .withColumn("ingest_file_name",input_file_name)
             .withColumn("ingested_at",current_timestamp())

customerDF2018.printSchema()


 

def replaceEmptyCols(columns:Array[String]):Array[Column]={

    columns.map(c=>{

      when(col(c)==="null" ,null).otherwise(col(c)).alias(c)

    })

}

customerDF2018=customerDF2018.select(replaceEmptyCols(customerDF2018.columns):_*)

customerDF2018.write

  .format("delta")

  .mode("append")

  .option("mergeSchema",true)

  .save("dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/batch_orders_dirty.delta")

 

// COMMAND ----------

// MAGIC %md ### Reality Check #2.B
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_02_b()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.C - Ingest Comma-Separted File</h2>
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC 1. Use the variable **`batch_2019_path`**, and **`dbutils.fs.head`** to investigate the 2019 batch file, if needed.
// MAGIC 2. Configure a **`DataFrameReader`** to ingest the comma-separated file identified by **`batch_2019_path`**
// MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
// MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
// MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created dataset specified by **`batch_target_path`**<br/>
// MAGIC    Note: The column names in this dataset must be updated to conform to the schema defined for Exercise #2.A - there are several strategies for this:
// MAGIC    * Provide a schema that alters the names upon ingestion
// MAGIC    * Manually rename one column at a time
// MAGIC    * Use **`fixed_width_column_defs`** programaticly rename one column at a time
// MAGIC    * Use transformations found in the **`DataFrame`** class to rename all columns in one operation
// MAGIC
// MAGIC **Additional Requirements**
// MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**<br/>

// COMMAND ----------

// MAGIC %md ### Implement Exercise #2.C
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------



import org.apache.spark.sql.functions.{trim,ltrim,rtrim,col}

import org.apache.spark.sql.functions.{col,when}

import org.apache.spark.sql._

import org.apache.spark.sql.functions.{input_file_name,current_timestamp}

import org.apache.spark.sql.types._


    

val batch_2019_path="dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/raw/orders/batch/2019.csv"

var customerDF2019 = spark.read.format("csv")

            .option("delimiter", ",")

            .option("header",true)

            .load(batch_2019_path)

            .withColumn("ingested_at",current_timestamp())

            .withColumn("ingest_file_name",input_file_name)



customerDF2019 = customerDF2019.withColumnRenamed("submittedAt","submitted_at")

.withColumnRenamed("orderId","order_id")

.withColumnRenamed("customerId","customer_id")

.withColumnRenamed("salesRepId","sales_rep_id")

.withColumnRenamed("salesRepSsn","sales_rep_ssn")

.withColumnRenamed("salesRepFirstName","sales_rep_first_name")

.withColumnRenamed("salesRepLastName","sales_rep_last_name")

.withColumnRenamed("salesRepAddress","sales_rep_address")

.withColumnRenamed("salesRepCity","sales_rep_city")

.withColumnRenamed("salesRepState","sales_rep_state")

.withColumnRenamed("salesRepZip","sales_rep_zip")

.withColumnRenamed("shippingAddressAttention","shipping_address_attention")

.withColumnRenamed("shippingAddressAddress","shipping_address_address")

.withColumnRenamed("shippingAddressCity","shipping_address_city")

.withColumnRenamed("shippingAddressState","shipping_address_state")

.withColumnRenamed("shippingAddressZip","shipping_address_zip")

.withColumnRenamed("productId","product_id")

.withColumnRenamed("productQuantity","product_quantity")

.withColumnRenamed("productSoldPrice","product_sold_price")



customerDF2019.show()



def replaceEmptyCols(columns:Array[String]):Array[Column]={

    columns.map(c=>{

      when(col(c)==="null" ,null).otherwise(col(c)).alias(c)

    })

}

customerDF2019=customerDF2019.select(replaceEmptyCols(customerDF2019.columns):_*)

customerDF2019.write

  .format("delta")

  .mode("append")

  .option("mergeSchema",true)

  .save("dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/batch_orders_dirty.delta")




// COMMAND ----------

// MAGIC %md ### Reality Check #2.C
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_02_c()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2 - Final Check</h2>
// MAGIC
// MAGIC Run the following command to make sure this exercise is complete:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_02_final()

// COMMAND ----------

