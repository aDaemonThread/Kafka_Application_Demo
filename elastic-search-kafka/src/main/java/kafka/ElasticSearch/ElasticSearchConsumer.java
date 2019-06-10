package kafka.ElasticSearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient client(){

        String hostname = "ankit-kafka-consumer-3301047150.ap-southeast-2.bonsaisearch.net";
        String username = "nz2v6a4tny";
        String password = "t59c7hw9i8";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = (RestClientBuilder) RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback((new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }));

        RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);
        return highLevelClient;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrap_server = "127.0.0.1:9092";
        String group_ID = "twitter-elastic-search";

        // Create Properties for Kafka Consumer

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        // Create Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe Consumer to Kafka Topics

        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        String topic = "twitter_topic";

        RestHighLevelClient client = client();

        KafkaConsumer<String, String> consumer = createConsumer(topic);

        while(true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            logger.info("Fetched:: " + records.count()+" records");
            for(ConsumerRecord<String, String> record : records){

                // One way to create unique ID for msg - Kafka Generic Way
                // String id = record.topic()+"_"+record.partition()+"_"+record.offset();

                // Create unique ID for tweets - for custom app
                String id = extractIDfromTweet(record.value());

                IndexRequest indexRequest = new IndexRequest("twitter_kafka", "tweets", id).source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                String status =indexResponse.getId();

                logger.info(status);

            }
            logger.info("Committing Consumer Offset");
            consumer.commitSync();
            logger.info("Offsets are committed");
        }

    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIDfromTweet(String tweetJson) {

         return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();

    }
}
