# Big-Data-Project
Full-Time Developer
![istockphoto-1455180758-1024x1024](https://github.com/user-attachments/assets/803da3b1-2710-44af-9b25-c9dee89b82fb)





#This project has three parts
##part 1) is stockdata pipe line containing the following components
##Kafka-producer
Listens to Stock Market Alpha Vantage API and sends stream of stockdata to kafka topics

##kafka-consumer
Is a spark streaming job the moniters stockdata that arrives at the kafka topics
It reads the data, change the json data to a format that is compatable with hbase table, and stores the data on an hbase table

##stockdata-related-scripts 
contain scrips used to run hbase servers, create hbase table, create hive table on top hbase, and query the data in hive shell

 ##part 2) is wordcount spark streaming job
 This program listens to a local directory, recieves text input, count the words in the currently arrived data and send the words and their count to     hbase table.
There a a hive table created on topo of hbase to access the words and their count in hive as well
Finally We visualize these information in Tableau
the following components are related to this pipe line: 
##wordcountSparkStreamingJob
##wordcount-related-scrips
  
 
