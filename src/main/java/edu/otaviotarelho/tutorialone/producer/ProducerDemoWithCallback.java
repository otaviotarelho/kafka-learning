package edu.otaviotarelho.tutorialone.producer;

import edu.otaviotarelho.secretsKeys.SecretKeys;
import edu.otaviotarelho.tutorialTwitter.TwitterProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        //Start kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(SecretKeys.DefoultProperties());
        ProducerRecord<String, String> record;


        //send
        for(int i = 0; i < 10; i++){
            //create producer record
            record = new ProducerRecord<String, String>("first_topic", "hallo world = " + i);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        log.info("Received Matadata: \n Topic :"  + recordMetadata.topic() +
                                "\n Partition: " + recordMetadata.partition() +
                                "\n Offset: " + recordMetadata.offset() +
                                "\n Timestemp " + recordMetadata.timestamp()
                        );

                    } else {
                        log.info("Error in production", e);
                    }
                }
            });
        }

        //flush and close producer
        producer.flush();
        producer.close();

    }
}
