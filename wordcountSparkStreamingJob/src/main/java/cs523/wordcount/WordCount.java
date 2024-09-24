package cs523.haaraa;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCount implements Serializable {  // Implement Serializable
    private static final long serialVersionUID = 1L;  // Add serialVersionUID to avoid warning
    private static final Pattern SPACE = Pattern.compile("\\W+");

    public static void main(String[] args) throws Exception {
        // Create Spark Streaming context
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FileStreamingExample");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Monitor a directory for new files
        JavaDStream<String> lines = ssc.textFileStream(args[0]);
        System.out.println("Monitoring directory: " + args[0]);

        lines.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("New batch of lines received:");
                System.out.println("File content received: " + rdd.collect());

                // Split each line into words using FlatMapFunction
                JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String line) {
                        return Arrays.asList(SPACE.split(line));
                    }
                });

                // Map each word to a (word, 1) pair and then reduce by key to get word counts
                JavaPairRDD<String, Integer> wordCounts = words
                        .mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1))
                        .reduceByKey(Integer::sum);

                // Save word counts to HBase
                wordCounts.foreachPartition(partition -> {
                    try (Connection connection = createHBaseConnection()) {
                        Table table = connection.getTable(TableName.valueOf("wordcount"));
                        partition.forEachRemaining(tuple -> {
                            String word = tuple._1();
                            Integer count = tuple._2();

                            // Create a new row in HBase for each word count
                            Put put = new Put(Bytes.toBytes(word));
                            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(count));

                            // Insert the data into the HBase table
                            try {
								table.put(put);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                            System.out.println("Inserted word: " + word + ", Count: " + count);
                        });
                        table.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                // Print the word counts
                wordCounts.collect().forEach(tuple -> {
                    System.out.println(tuple._1() + ": " + tuple._2());
                });
            } else {
                System.out.println("No new lines received in this batch.");
            }
        });

        // Start the computation
        ssc.start();
        ssc.awaitTermination();
    }

    // Method to create HBase connection inside workers (and avoid serialization issues)
    private static Connection createHBaseConnection() throws Exception {
        Configuration hbaseConfig = HBaseConfiguration.create();
        return ConnectionFactory.createConnection(hbaseConfig);
    }
}
