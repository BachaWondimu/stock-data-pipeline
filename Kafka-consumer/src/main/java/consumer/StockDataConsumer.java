package consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class StockDataConsumer {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Initialize Spark streaming context
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaSparkConsumer")
                .setMaster("local[*]")
                .set("spark.ui.port", "4050"); // Specify an available port here
        
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(60000));

        // Define Kafka parameters
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092"); // Updated to use bootstrap.servers

        // Define the topic to consume
        Set<String> topics = new HashSet<>();
        topics.add("stock_market_data");

        // Create a direct stream from Kafka
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        // Process the stream
        JavaDStream<String> messages = kafkaStream.map(Tuple2::_2);

       

     // Use foreachRDD with foreachPartition to process partitions efficiently
        messages.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                // Create HBase connection inside the partition
                Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "localhost");
                config.set("hbase.zookeeper.property.clientPort", "2181");

                try (Connection hbaseConnection = ConnectionFactory.createConnection(config);
                     Table table = hbaseConnection.getTable(TableName.valueOf("stock2"))) {

                    partition.forEachRemaining(message -> {
                        System.out.println("Received message: " + message);

                        try {
                            // Parse the JSON message
                            ObjectMapper objectMapper = new ObjectMapper();
                            JsonNode rootNode = objectMapper.readTree(message);

                            // Check if both "Meta Data" and "Time Series (1min)" exist
                            if (rootNode.has("Meta Data") && rootNode.has("Time Series (1min)")) {
                                JsonNode timeSeriesNode = rootNode.get("Time Series (1min)");

                                Iterator<Map.Entry<String, JsonNode>> entries = timeSeriesNode.fields();

                                // Iterate over each timestamp in the JSON
                                while (entries.hasNext()) {
                                    Map.Entry<String, JsonNode> entry = entries.next();
                                    String tradeTime = entry.getKey();
                                    JsonNode dataNode = entry.getValue();

                                    // Handle missing or null fields
                                    String openPrice = dataNode.has("1. open") ? dataNode.get("1. open").asText() : null;
                                    String highPrice = dataNode.has("2. high") ? dataNode.get("2. high").asText() : null;
                                    String lowPrice = dataNode.has("3. low") ? dataNode.get("3. low").asText() : null;
                                    String closePrice = dataNode.has("4. close") ? dataNode.get("4. close").asText() : null;
                                    String volume = dataNode.has("5. volume") ? dataNode.get("5. volume").asText() : null;

                                    // Ensure non-null values before creating the Put object
                                    if (openPrice != null && highPrice != null && lowPrice != null && closePrice != null && volume != null) {
                                        // Parse and cast data types for HBase storage
                                        Put put = new Put(Bytes.toBytes(tradeTime));
                                        put.addColumn(Bytes.toBytes("prices"), Bytes.toBytes("open_price"), Bytes.toBytes(String.valueOf(openPrice)));
                                        put.addColumn(Bytes.toBytes("prices"), Bytes.toBytes("high_price"), Bytes.toBytes(String.valueOf(highPrice)));
                                        put.addColumn(Bytes.toBytes("prices"), Bytes.toBytes("low_price"), Bytes.toBytes(String.valueOf(lowPrice)));
                                        put.addColumn(Bytes.toBytes("prices"), Bytes.toBytes("close_price"), Bytes.toBytes(String.valueOf(closePrice)));
                                        put.addColumn(Bytes.toBytes("prices"), Bytes.toBytes("trade_volume"), Bytes.toBytes(String.valueOf(volume)));

                                        table.put(put);                            
                                        System.out.println("Data inserted successfully for timestamp: " + tradeTime);
                                    } else {
                                        System.err.println("Skipping entry due to missing data at: " + tradeTime);
                                    }
                                }
                            } else {
                                System.err.println("Skipping message due to missing 'Meta Data' or 'Time Series (1min)' fields");
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON or processing data: " + e.getMessage());
                            e.printStackTrace();
                        }
                    });
                } catch (IOException e) {
                    System.err.println("Error connecting to HBase: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        });


        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
