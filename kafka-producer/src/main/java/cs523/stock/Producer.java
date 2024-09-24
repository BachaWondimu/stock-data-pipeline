package cs523.stock;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;




import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class Producer {

    private static final String TOPIC = "stock_market_data";
    private static final String API_KEY = "SOfw5hpGK0YP9b4aikgLpYDIyNJpINvj";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String SYMBOL = "AAPL";

    public static void main(String[] args) {
        // Kafka properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Kafka producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
             CloseableHttpClient httpClient = HttpClients.createDefault()) {

            ObjectMapper objectMapper = new ObjectMapper();

            while (true) {
                // Fetch stock data
                String url = String.format("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=1min&apikey=%s", SYMBOL, API_KEY);
                HttpGet request = new HttpGet(url);
                HttpResponse response = httpClient.execute(request);

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    StringBuilder stringBuilder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stringBuilder.append(line);
                    }

                    String stockData = stringBuilder.toString();
                    System.out.println("Fetched Stock Data: " + stockData);

                    // Send stock data to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, stockData);
                    producer.send(record);
                }

                // Wait 10 seconds before fetching again
                Thread.sleep(10000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

