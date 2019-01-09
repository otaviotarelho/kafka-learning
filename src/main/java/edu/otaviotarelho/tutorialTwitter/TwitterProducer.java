package edu.otaviotarelho.tutorialTwitter;

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
import edu.otaviotarelho.secretsKeys.SecretKeys;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Twitter docs
 * https://github.com/twitter/hbc
 */
public class TwitterProducer {

    private Authentication hosebirdAuth;
    private Hosts hosebirdHosts;
    private StatusesFilterEndpoint hosebirdEndpoint;
    private BlockingQueue<String> msgQueue;
    private Logger log = LoggerFactory.getLogger(TwitterProducer.class);
    private KafkaProducer<String, String> producer;
    private List<String> terms = null;
    public TwitterProducer() {
        terms = Lists.newArrayList("java", "ucdavis",  "c++", "c#", "programming", "computer sciente");
    }

    private void run() {
        //create a twitter cliente
        hosebirdAuth = Auth();

        Client cliente = createCliente();
        cliente.connect();

        //create a Kafka producer
        producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("finishing session");
            cliente.stop();
            producer.close();
        }));

        SendMessage(cliente);
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        //create properties
        Properties properties = SecretKeys.DefoultProperties();

        //creating safer producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //better batch size
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //Start kafka producer
        return new KafkaProducer<String, String>(properties);

    }



    private void SendMessage(Client cliente) {
        //loop and send twitter
        while (!cliente.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cliente.stop();
            }

            if(msg != null){
                log.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            log.error("Something happened", e);
                        }
                    }
                });
            }
        }

        log.info("App ended");
    }

    private Client createCliente() {
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

       return builder.build();
    }

    private OAuth1 Auth() {
        msgQueue = new LinkedBlockingQueue<String>(100000);

        hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        return new OAuth1(SecretKeys.CONSUMER_KEY, SecretKeys.CONSUMER_SECRET, SecretKeys.TOKEN_KEY, SecretKeys.TOKEN_SECRET);
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

}
