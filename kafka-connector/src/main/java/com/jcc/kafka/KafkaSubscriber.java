package com.jcc.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public abstract class KafkaSubscriber implements Runnable {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaSubscriber.class);
	
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer = null;
    
    protected abstract void procesarMensaje (String msg);

	protected void run(String boostrapServers, String topic, String consumerGroupId) {
		log.info(String.format("Iniciando KafkaSubscriber"));
		consumer = new KafkaConsumer<String, String>(generateConfig(boostrapServers, consumerGroupId));
		
        try {
    		consumer.subscribe(Arrays.asList(topic)); // Arrays.asList("foo", "bar"));
    		
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
				for (ConsumerRecord<String, String> record : records) {
					log.debug(String.format("KFK mensaje recibido (%s) - offset: %d, key: %s, value: %s", topic, record.offset(), record.key(), record.value()));
					try {
						procesarMensaje (record.value());
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
            }

        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
			log.info("Detenido KafkaSubscriber");
        }
    }

	private Properties generateConfig(String boostrapServers, String consumerGroupId) {
		Properties props = new Properties();
		props.put("bootstrap.servers", boostrapServers);
		props.put("group.id", consumerGroupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
    	log.info("shutdown KafkaSubscriber");
        closed.set(true);
        consumer.wakeup();
    }
}
