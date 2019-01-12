package edu.otaviotarelho.tutorialone.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CosumerDemoAssignAndSeek {

    public static Logger log = LoggerFactory.getLogger(CosumerDemoAssignAndSeek.class.getName());

    public static void main(String[] args) {

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        final TopicPartition partition = new TopicPartition("first_topic", 0);
        final long offsetReadFrom = 15L;
        final int numberOfMessageToRead = 5;
        int numberOfMessageRead = 0;
        boolean keepReading = true;
        consumer.assign(Arrays.asList(partition));

        consumer.seek(partition, offsetReadFrom);

        while (keepReading){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record : records){
                numberOfMessageRead += 1;

                log.info("Key:" + record.key());
                log.info("Value:" + record.value());
                log.info("partition:" + record.partition());
                log.info("offset:" + record.offset());

                if(numberOfMessageRead >= 5){
                    keepReading = false;
                    break;
                }
            }

            log.info("Exisiting app");
        }

    }
}
