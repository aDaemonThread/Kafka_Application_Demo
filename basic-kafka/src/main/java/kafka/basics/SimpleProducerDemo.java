package kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerDemo {


    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(SimpleProducerDemo.class);
        String bootstrap_server = "127.0.0.1:9092";
        String topic = "first_topic";

        //Create Kafka Producer Properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Kafka Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "0","Second Message");

        // Send data to Kafka Topic

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e!=null){
                    logger.error("Error while sending Data" + e);
                }
                else {
                    logger.info("Msg Sent Successfully \n" + "Partition: " + recordMetadata.partition() + " Time: " + recordMetadata.timestamp());
                }
            }
        });
        producer.close();
    }
}
