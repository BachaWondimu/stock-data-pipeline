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

  
 
