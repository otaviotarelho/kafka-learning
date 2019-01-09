package edu.otaviotarelho.tutorialone.producer;

import edu.otaviotarelho.secretsKeys.SecretKeys;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

    public static void main(String[] args) {

        //Start kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(SecretKeys.DefoultProperties());

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hallo world");

        //send
        producer.send(record);

        //flush and close producer
        producer.flush();
        producer.close();

    }
}
