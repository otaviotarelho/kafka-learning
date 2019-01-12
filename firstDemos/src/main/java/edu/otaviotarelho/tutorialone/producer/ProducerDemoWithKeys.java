package edu.otaviotarelho.tutorialone.producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    private static Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //Start kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(SecretKeys.DefoultProperties());
        ProducerRecord<String, String> record;

        String topic = "first_topic";

        //send
        for(int i = 0; i < 10; i++){
            //create producer record
            String helloworld = "hello world = " + i;
            String key = "id_" + i;

            record = new ProducerRecord<String, String>(topic, key, helloworld);
            log.info("key:" + key);
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
            }).get(); //block the sender to be sync, dont do it in production
        }

        //flush and close producer
        producer.flush();
        producer.close();

    }
}
