package org.example.develop.brokerclient.kafka.connector;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KafkaPublisher {

    private static Producer<String, String> producer;
    private static boolean initFlag = false;

    public static void publish(String boostrapServers, String topic, String payload) {
        if (!initFlag)
            initKafkaPublisher(boostrapServers);

        log.debug(String.format("KFK publishing message (%s): %s", topic, payload.replace('\r', ' ').replace('\n', ' ')));

        producer.send(new ProducerRecord<String, String>(topic, payload));
        producer.flush();
    }

    private static void initKafkaPublisher(String boostrapServers) {
        if (!initFlag) {
            Properties props = new Properties();
            props.put("bootstrap.servers", boostrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            log.debug("Starting KafkaPublisher");

            producer = new KafkaProducer<>(props);
            KafkaPublisher.initFlag = true;
        }
    }

    public static void shutdown() {
        log.debug("shutdown KafkaPublisher");
        if (initFlag) {
            producer.flush();
            producer.close();
        }
    }
}