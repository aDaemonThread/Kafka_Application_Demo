package kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumerDemo {

    public SimpleConsumerDemo() {
    }

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(SimpleConsumerDemo.class);
        String bootstrap_server = "127.0.0.1:9092";
        String topic = "first_topic";
        String group_ID = "Sample_Application";

        // Create Properties for Kafka Consumer

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe Consumer to Kafka Topics

        consumer.subscribe(Arrays.asList(topic));

        // Poll for new Data

        while(true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records){
                logger.info("\nTopic: " + record.topic() + "\n" + "Key: " + record.key() + "\n" + "Value: " + record.value() );
            }
        }
    }
}
