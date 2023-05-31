// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md 
// MAGIC # Exercise #6 - Business Questions
// MAGIC
// MAGIC In our last exercise, we are going to execute various joins across our four tables (**`orders`**, **`line_items`**, **`sales_reps`** and **`products`**) to answer basic business questions
// MAGIC
// MAGIC This exercise is broken up into 3 steps:
// MAGIC * Exercise 6.A - Use Database
// MAGIC * Exercise 6.B - Question #1
// MAGIC * Exercise 6.C - Question #2
// MAGIC * Exercise 6.D - Question #3

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #6</h2>
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

// MAGIC %run ./_includes/Setup-Exercise-06

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.A - Use Database</h2>
// MAGIC
// MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
// MAGIC
// MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

// COMMAND ----------

// MAGIC %md ### Implement Exercise #6.A
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

// MAGIC %sql
// MAGIC USE dbacademy_pramod_dm_lntinfotech_com_db

// COMMAND ----------

// MAGIC %md ### Reality Check #6.A
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_06_a()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.B - Question #1</h2>
// MAGIC ## How many orders were shipped to each state?
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Aggregate the orders by **`shipping_address_state`**
// MAGIC * Count the number of records for each state
// MAGIC * Sort the results by **`count`**, in descending order
// MAGIC * Save the results to the temporary view **`question_1_results`**, identified by the variable **`question_1_results_table`**

// COMMAND ----------

// MAGIC %md ### Implement Exercise #6.B
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

var  dd=spark.sql("SELECT count(order_id) as count,shipping_address_state from Orders Group By shipping_address_state Order By count desc")
dd.createOrReplaceTempView("question_1_results")




// COMMAND ----------

// MAGIC %md ### Reality Check #6.B
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_06_b()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.C - Question #2</h2>
// MAGIC ## What is the average, minimum and maximum sale price for green products sold to North Carolina where the Sales Rep submitted an invalid Social Security Number (SSN)?
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Execute a join across all four tables:
// MAGIC   * **`orders`**, identified by the variable **`orders_table`**
// MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
// MAGIC   * **`products`**, identified by the variable **`products_table`**
// MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
// MAGIC * Limit the result to only green products (**`color`**).
// MAGIC * Limit the result to orders shipped to North Carolina (**`shipping_address_state`**)
// MAGIC * Limit the result to sales reps that initially submitted an improperly formatted SSN (**`_error_ssn_format`**)
// MAGIC * Calculate the average, minimum and maximum of **`product_sold_price`** - do not rename these columns after computing.
// MAGIC * Save the results to the temporary view **`question_2_results`**, identified by the variable **`question_2_results_table`**
// MAGIC * The temporary view should have the following three columns: **`avg(product_sold_price)`**, **`min(product_sold_price)`**, **`max(product_sold_price)`**
// MAGIC * Collect the results to the driver
// MAGIC * Assign to the following local variables, the average, minimum, and maximum values - these variables will be passed to the reality check for validation.
// MAGIC  * **`ex_avg`** - the local variable holding the average value
// MAGIC  * **`ex_min`** - the local variable holding the minimum value
// MAGIC  * **`ex_max`** - the local variable holding the maximum value

// COMMAND ----------

// MAGIC %md ### Implement Exercise #6.C
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

import org.apache.spark.sql.functions.{col,when,avg,max,min}
var orders= spark.sql("select * from orders")
var line_items= spark.sql("select * from line_items")
var products= spark.sql("select * from products")
var sales_reps= spark.sql("select * from sales_reps")
 
 
var  dfD=spark.sql("""SELECT *
FROM ((orders INNER JOIN sales_reps ON orders.sales_rep_id = sales_reps.sales_rep_id)
         INNER JOIN line_items ON orders.order_id = line_items.order_id)
          INNER JOIN products ON line_items.product_id = products.product_id""")
dfD=dfD.filter($"color"==="green").filter($"shipping_address_state"==="NC")
.filter($"_error_ssn_format"===true).agg(avg($"product_sold_price"),min($"product_sold_price"),max($"product_sold_price"))
 
dfD.createOrReplaceTempView("question_2_results")
var question_2_results = dfD.collect 
 

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, avg, min, max
// MAGIC exp_results = spark.read.table(orders_table).join(spark.read.table(sales_reps_table), "sales_rep_id").join(spark.read.table(line_items_table), "order_id").join(spark.read.table(products_table), "product_id").filter(col("shipping_address_state") == "NC").filter(col("_error_ssn_format") == True).filter(col("color") == "green").select(avg("product_sold_price"), min("product_sold_price"), max("product_sold_price")).first()
// MAGIC ex_avg = exp_results["avg(product_sold_price)"]
// MAGIC ex_min = exp_results["min(product_sold_price)"]
// MAGIC ex_max = exp_results["max(product_sold_price)"]

// COMMAND ----------

// MAGIC %md ### Reality Check #6.C
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_06_c(ex_avg, ex_min, ex_max)

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.D - Question #3</h2>
// MAGIC ## What is the first and last name of the top earning sales rep based on net sales?
// MAGIC
// MAGIC For this scenario...
// MAGIC * The top earning sales rep will be identified as the individual producing the largest profit.
// MAGIC * Profit is defined as the difference between **`product_sold_price`** and **`price`** which is then<br/>
// MAGIC   multiplied by **`product_quantity`** as seen in **(product_sold_price - price) * product_quantity**
// MAGIC
// MAGIC **In this step you will need to:**
// MAGIC * Execute a join across all four tables:
// MAGIC   * **`orders`**, identified by the variable **`orders_table`**
// MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
// MAGIC   * **`products`**, identified by the variable **`products_table`**
// MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
// MAGIC * Calculate the profit for each line item of an order as described above.
// MAGIC * Aggregate the results by the sales reps' first &amp; last name and then sum each reps' total profit.
// MAGIC * Reduce the dataset to a single row for the sales rep with the largest profit.
// MAGIC * Save the results to the temporary view **`question_3_results`**, identified by the variable **`question_3_results_table`**
// MAGIC * The temporary view should have the following three columns: 
// MAGIC   * **`sales_rep_first_name`** - the first column by which to aggregate by
// MAGIC   * **`sales_rep_last_name`** - the second column by which to aggregate by
// MAGIC   * **`sum(total_profit)`** - the summation of the column **`total_profit`**

// COMMAND ----------

// MAGIC %md ### Implement Exercise #6.D
// MAGIC
// MAGIC Implement your solution in the following cell:

// COMMAND ----------

import org.apache.spark.sql.functions._


var  dfD=spark.sql("""SELECT *
FROM ((orders INNER JOIN sales_reps ON orders.sales_rep_id = sales_reps.sales_rep_id)
         INNER JOIN line_items ON orders.order_id = line_items.order_id)
          INNER JOIN products ON line_items.product_id = products.product_id""")


dfD=dfD.withColumn("per_product_profit", col("product_sold_price") - col("price"))
.withColumn("total_profit", col("per_product_profit") * col("product_quantity"))
.groupBy( "sales_rep_first_name", "sales_rep_last_name").sum("total_profit")
.orderBy(desc("sum(total_profit)"))//.take(1)

dfD.collect()
dfD.createOrReplaceTempView("question_results")
dfD=spark.sql("""select * from question_results where sales_rep_first_name="River"""")
dfD.createOrReplaceTempView("question_3_results")
dfD.printSchema()
  display(dfD)
var exp_first = "River"
var exp_last = "Spears"

// COMMAND ----------

// MAGIC %md ### Reality Check #6.D
// MAGIC Run the following command to ensure that you are on track:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_06_d()

// COMMAND ----------

// MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6 - Final Check</h2>
// MAGIC
// MAGIC Run the following command to make sure this exercise is complete:

// COMMAND ----------

// MAGIC %python
// MAGIC reality_check_06_final()

// COMMAND ----------

