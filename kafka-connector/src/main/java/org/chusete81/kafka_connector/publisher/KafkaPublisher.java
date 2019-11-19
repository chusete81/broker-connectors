package org.chusete81.kafka_connector.publisher;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPublisher {

	private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

	private static Producer<String, String> producer;
	private static boolean initFlag = false;
	
	public static void publish(String boostrapServers, String topic, String payload) {
		if (!initFlag)
			initKafkaPublisher(boostrapServers);
		
		log.debug(String.format("KFK publicando mensaje (%s): %s", topic, payload.replace('\r', ' ').replace('\n', ' ')));
		
		producer.send(new ProducerRecord<String, String>(topic, payload));
		producer.flush();
	}

	private static void initKafkaPublisher(String boostrapServers) {
		if (!initFlag) {
			Properties props = new Properties();
			props.put("bootstrap.servers", boostrapServers);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			
			log.info("Inicializando KafkaPublisher");

			producer = new KafkaProducer<>(props);
			KafkaPublisher.initFlag = true;
		}
	}

	public static void shutdown() {
		log.info("shutdown KafkaPublisher");
		if (initFlag) {
			producer.flush();
			producer.close();
		}
	}
}
