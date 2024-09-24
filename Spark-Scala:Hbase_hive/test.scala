// Initialize SQLContext or HiveContext depending on the use case
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

// Show databases
hiveContext.sql("SHOW DATABASES").show()

// Use a specific database (optional)
hiveContext.sql("USE DEFAULT")

// Show tables in the selected database
hiveContext.sql("SHOW TABLES").show()

// Print schema
df.printSchema()

// Query a specific table
val df = hiveContext.sql("SELECT * FROM commodities  LIMIT 10")

// Show the results
df.show()

//query

val df2 = hiveContext.sql("SELECT price_1 FROM commodities WHERE price_1 >100.00")

df2.show()

//query 
// Get distinct price dates where price_2 is not null
val df4 = hiveContext.sql("SELECT DISTINCT price_date, price_1 FROM commodities")

df4.show()



