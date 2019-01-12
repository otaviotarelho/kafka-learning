package edu.otaviotarelho.tutorialone.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CosumerDemoWithThreads {

    public static Logger log = LoggerFactory.getLogger(CosumerDemoWithThreads.class.getName());

    public static void main(String[] args) {
        new CosumerDemoWithThreads().run();
    }

    private CosumerDemoWithThreads(){

    }

    public void run(){
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerTheadRunnable = new ConsumerRunnable(new CountDownLatch(1), "localhost:9092", "my-sixth-app", "first_topic");
        Thread myThead = new Thread(consumerTheadRunnable);
        myThead.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("shutdown hook");
            ((ConsumerRunnable) consumerTheadRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupected", e);
        } finally {
            log.info("app finishing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private final String bootstrapServer;
        private final String groupId;
        private final String topic;
        private KafkaConsumer<String, String> consumer;
        private Properties properties;
        public Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServer, String groupId, String topic){
            this.latch = latch;
            this.bootstrapServer = bootstrapServer;
            this.groupId = groupId;
            this.topic = topic;
            this.properties = new Properties();
        }

        @Override
        public void run() {
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Collections.singleton(topic));

            try{
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        log.info("Key:" + record.key());
                        log.info("Value:" + record.value());
                        log.info("partition:" + record.partition());
                        log.info("offset:" + record.offset());
                    }
                }
            } catch (WakeupException e){
                log.info("Received shutdown signal!");
            }
             finally {
                consumer.close();
                latch.countDown(); // tell main code that we are done with the consumer..
            }

        }

        public void shutdown(){
            //special method to interrupt consumer.poll()
            consumer.wakeup();
        }
    }
}
