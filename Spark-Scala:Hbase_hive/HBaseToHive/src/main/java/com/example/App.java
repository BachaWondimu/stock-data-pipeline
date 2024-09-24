package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class App {
    public static void main(String[] args) {
        // Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("HBaseToHive")
                .setMaster("local[*]");  // Use local mode for testing

        // Initialize Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Initialize HiveContext to interact with Hive tables
        HiveContext hiveContext = new HiveContext(sc);

        // Run a simple SQL query on Hive
        try {
            DataFrame df = hiveContext.sql("SHOW TABLES;");
            df.show();
        } catch (Exception e) {
            System.err.println("Exception while running Hive query: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the Spark context
            sc.stop();
        }
    }
}