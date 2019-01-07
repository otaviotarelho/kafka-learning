package edu.otaviotarelho.tutorialTwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import edu.otaviotarelho.secretsKeys.SecretKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

    public TwitterProducer() {

    }

    private void run() {
        //create a twitter cliente
        hosebirdAuth = Auth();

        Client cliente = createCliente();
        cliente.connect();

        //create a Kafka producer
        SendMessage(cliente);
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
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        return new OAuth1(SecretKeys.CONSUMER_KEY, SecretKeys.CONSUMER_SECRET, SecretKeys.TOKEN_KEY, SecretKeys.TOKEN_SECRET);
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

}
