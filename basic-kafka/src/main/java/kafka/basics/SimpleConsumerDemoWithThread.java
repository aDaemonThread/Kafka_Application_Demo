package kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SimpleConsumerDemoWithThread {

    private SimpleConsumerDemoWithThread() {
    }

    public static void main(String[] args) {

        new SimpleConsumerDemoWithThread().run();

    }

    private void run(){

        String bootstrap_server = "127.0.0.1:9092";
        String topic = "first_topic";
        String group_ID = "Sample_Application";
        Logger logger = LoggerFactory.getLogger(SimpleConsumerDemoWithThread.class);
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Starting Consumer Thread");
        Runnable myConsumerThread = new ConsumerThread(latch, bootstrap_server,topic,group_ID);

        // Start the Thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // Add a Shutdown Hook

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Invoked Shutdown Hook");
            ((ConsumerThread) myConsumerThread).shutdown();
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got Interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(CountDownLatch latch, String bootstrap_server, String topic, String group_ID) {

            this.latch=latch;

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
        }

        @Override
        public void run() {

            try {

                // Poll for new Data

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("\nTopic: " + record.topic() + "\n" + "Key: " + record.key() + "\n" + "Value: " + record.value());
                    }
                }
            }
            catch (WakeupException e){
                logger.info("Received SIGTERM");
            }
            finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {

            //wakeup() method will interrupt the consumer.poll()
            consumer.wakeup();

        }

    }
}