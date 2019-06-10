package kafka.Twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public TwitterProducer() {
    }

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    String consumerKey = "Pe8YLSOFAHEgYrH4MA43oHCnM";
    String consumerSecret = "bw3bDqJcmm2zJw8kbdYaVFi6ooeMwsijKs7vynAeja6lejuoeY";
    String token = "802255273-JZfLn6jIRhzZABa97X2pC7n9XlHMH1X1SFLNGbbF";
    String secret = "Ts9S4ZYyBUfypAzaUDzaALZ5aWrqRg6zUYrKleBIHICku";

    //String bootstrap_server = "127.0.0.1:9092";
    String bootstrap_server = "host-10-17-103-171.coe.cloudera.com:9092";
    String topic = "twitter_topic";

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run(){

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create Twitter Client
        Client client = TwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // Create Kafka Producer

        KafkaProducer<String, String> producer = createKafkaProducer();

        // loop to send tweets to Kafka

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info("Msg from Twitter: " + msg);
                producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Error while sending Data" + e);
                        }
                    }
                });
            }
        }
        logger.info("Application Shutdown");
    }

    public Client TwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList( "cloudera", "bigdata", "kafka", "spark", "AWS");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Ankit-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer(){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Kafka Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }
}
