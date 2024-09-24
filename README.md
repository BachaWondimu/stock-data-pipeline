# Big-Data-Project
Full-Time Developer
![istockphoto-1455180758-1024x1024](https://github.com/user-attachments/assets/803da3b1-2710-44af-9b25-c9dee89b82fb)





# Project Overview

This project is divided into three main parts, each containing different components as described below:

## Part 1: Stock Data Pipeline
This pipeline processes stock market data using the following components:

### Kafka Producer
- Listens to the Alpha Vantage Stock Market API and sends a stream of stock data to Kafka topics.

### Kafka Consumer
- A Spark Streaming job that monitors stock data arriving in Kafka topics.
- Reads the data, converts the JSON format to be compatible with an HBase table, and stores the data in HBase.

### Stock Data-Related Scripts
- Contains scripts for managing HBase servers, creating HBase tables, creating Hive tables on top of HBase, and querying the data in Hive.

## Part 2: Word Count Spark Streaming Job
This part is focused on processing word count data:

### Word Count Streaming Job
- Listens to a local directory, receives text input, counts the words in the incoming data, and stores the word count in an HBase table.
- A Hive table is created on top of HBase to access the words and their counts via Hive.
- The results are visualized using Tableau.

### Word Count-Related Scripts
- Includes scripts to set up and manage the Word Count Spark Streaming job, as well as related data processing tasks.

## Spark SQL AND Hbase/hive
- you can find the command steps to run the spark Scala/SQL -> Folder(Spark-Scala:Hbase_hive)
### Hive and Spark Integration
- Objective: You're using Spark SQL to query a Hive table called commodities. This table contains information about different commodities, their prices, and the date of the prices.
- Spark's Role: Spark is used to perform data processing tasks (e.g., calculating average prices of commodities per year).
- Hive's Role: Hive acts as your data warehouse, where the raw commodity data is stored and queried using SQL.

### Overall Workflow:
- Data Querying: Spark queries data from the Hive table (commodities) and calculates the yearly average price of each commodity.
- Data Transformation: Spark processes this data by calculating the required aggregations, such as average price per year for each commodity.
- Data Insertion into HBase: Once the results are ready, you use an HBase connection to store these results into the HBase table (commodity_prices). Each entry in this table represents a commodity-year pair with the associated average price.
- Final Goal: The goal is to create an efficient pipeline that retrieves and processes data from Hive (your batch data warehouse) and stores the results in HBase (a real-time NoSQL database). This allows you to run further real-time queries and analyses on this data, leveraging HBase's speed.

### Why This Project Is Useful
   This project teaches you how to:

- Leverage Distributed Data Processing: Use Spark to query and process large datasets.
- Integrate Hive and Spark: Perform SQL-like queries on large datasets stored in Hive.
- Integrate Spark with HBase: Insert processed data from Spark into HBase, combining batch and real-time data storage systems.
- Real-Time Data Analytics: Prepare the foundation for real-time data retrieval and analysis, which can be useful for real-time dashboards or fast querying systems.