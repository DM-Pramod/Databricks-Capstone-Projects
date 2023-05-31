// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md 
// MAGIC # Exercise #4 - XML Ingestion, Products Table
// MAGIC
// MAGIC The products being sold by our sales reps are itemized in an XML document which we will need to load.
// MAGIC
// MAGIC Unlike CSV, JSON, Parquet, & Delta, support for XML is not included with the default distribution of Apache Spark.
// MAGIC
// MAGIC Before we can load the XML document, we need additional support for a **`DataFrameReader`** that can processes XML files.
// MAGIC
// MAGIC Once the **spark-xml** library is installed to our cluster, we can load our XML document and proceede with our other transformations.
// MAGIC
// MAGIC This exercise is broken up into 4 steps:
// MAGIC * Exercise 4.A - Use Database
// MAGIC * Exercise 4.B - Install Library
// MAGIC * Exercise 4.C - Load Products
// MAGIC * Exercise 4.D - Load ProductLineItems

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #4</h2>
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

// MAGIC %run ./_includes/Setup-Exercise-04

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.A - Use Database</h2>
// MAGIC
// MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
// MAGIC
// MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

// COMMAND ----------

// MAGIC %md ### Implement Exercise #4.A
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

// MAGIC %sql
// MAGIC USE dbacademy_pramod_dm_lntinfotech_com_db

// COMMAND ----------

// MAGIC %md ### Reality Check #4.A
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_04_a()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.B - Install Library</h2>
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Register the **spark-xml** library - edit your cluster configuration and then from the **Libraries** tab, install the following library:
// MAGIC   * Type: **Maven**
// MAGIC   * Coordinates: **com.databricks:spark-xml_2.12:0.10.0**
// MAGIC
// MAGIC If you are unfamiliar with this processes, more information can be found in the <a href="https://docs.databricks.com/libraries/cluster-libraries.html" target="_blank">Cluster libraries documentation</a>.
// MAGIC
// MAGIC Once the library is installed, run the following reality check to confirm proper installation.<br/>
// MAGIC Note: You may need to restart the cluster after installing the library for you changes to take effect.

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_04_b()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.C - Load Products</h2>
// MAGIC
// MAGIC With the **spark-xml** library installed, ingesting an XML document is identical to ingesting any other dataset - other than specific, provided, options.
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Load the XML document using the following paramters:
// MAGIC   * Format: **xml**
// MAGIC   * Options:
// MAGIC     * **`rootTag`** = **`products`** - identifies the root tag in the XML document, in our case this is "products"
// MAGIC     * **`rowTag`** = **`product`** - identifies the tag of each row under the root tag, in our case this is "product"
// MAGIC     * **`inferSchema`** = **`True`** - The file is small, and a one-shot operation - infering the schema will save us some time
// MAGIC   * File Path: specified by the variable **`products_xml_path`**
// MAGIC   
// MAGIC * Update the schema to conform to the following specification:
// MAGIC   * **`product_id`**:**`string`**
// MAGIC   * **`color`**:**`string`**
// MAGIC   * **`model_name`**:**`string`**
// MAGIC   * **`model_number`**:**`string`**
// MAGIC   * **`base_price`**:**`double`**
// MAGIC   * **`color_adj`**:**`double`**
// MAGIC   * **`size_adj`**:**`double`**
// MAGIC   * **`price`**:**`double`**
// MAGIC   * **`size`**:**`string`**
// MAGIC
// MAGIC * Exclude any records for which a **`price`** was not included - these represent products that are not yet available for sale.
// MAGIC * Load the dataset to the managed delta table **`products`** (identified by the variable **`products_table`**)

// COMMAND ----------

// MAGIC %md ### Implement Exercise #4.C
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

/*
//import com.databricks.spark.xml._ // Add the DataFrame.read.xml() method
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val products_xml_path = "dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/raw/products/products.xml"

var df = spark.read
  .format("com.databricks.spark.xml")
  .option("rootTag", "products")
  .option("rowTag", "product")
  .option("inferSchema", "true")
  .load(products_xml_path)


//Function to flatten the STRUCT fields so that we can change the column name usd
def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)
       f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName))
      }
    })
  }
  df = df.select(flattenStructSchema(df.schema):_*)

df = df.withColumn("product_id",$"_product_id").drop($"_product_id")
           .withColumn("base_price",$"_base_price").drop($"_base_price")
           .withColumn("color_adj",$"_color_adj").drop($"_color_adj")
           .withColumn("size_adj",$"_size_adj").drop($"_size_adj")
           .withColumn("price",$"usd").drop($"usd")
  df=df.filter($"price".isNotNull)


spark.sql("drop table products")
df.write.format("delta").saveAsTable("products")
*/ 


//import com.databricks.spark.xml._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
val products_xml_path = "dbfs:/user/pramod.dm@lntinfotech.com/dbacademy/developer-foundations-capstone/raw/products/products.xml"

 

var df = spark.read
              .format("xml")
              .option("rootTag","products")
              .option("rowTag","product")
              .option("inferSchema",true)
              .load(products_xml_path)
var df_new = df 

 

def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

 

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName))
      }
    })
  }
  df = df.select(flattenStructSchema(df.schema):_*)
  df = df.withColumn("product_id",$"_product_id").drop($"_product_id")
           .withColumn("base_price",$"_base_price").drop($"_base_price")
           .withColumn("color_adj",$"_color_adj").drop($"_color_adj")
           .withColumn("size_adj",$"_size_adj").drop($"_size_adj")
           .withColumn("price",$"usd").drop($"usd")
  df=df.filter($"price".isNotNull)
  //spark.sql("drop table products")
  df.write.format("delta").mode("overwrite").saveAsTable("products")
 
  



// COMMAND ----------

// MAGIC %md ### Reality Check #4.C
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_04_c()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4 - Final Check</h2>
// MAGIC
// MAGIC Run the following command to make sure this exercise is complete:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_04_final()

// COMMAND ----------

