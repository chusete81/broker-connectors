package org.example.develop.brokerclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.example.develop.brokerclient.kafka.connector.KafkaSubscriber;

import java.util.Date;

@Slf4j
public class KafkaConsumer extends KafkaSubscriber
{
    private String bootstrapServer;
    private String topic;
    private String consumerGroupId;
    private int numMessages = 0;

    @Override
    protected void procesarMensaje (String msg)
    {
        log.info("[" + ++numMessages +"] " + new Date().toString() + " - msgRcvd: " +
                msg.substring(0, Math.min(8, msg.length())) + " (" + msg.length() + " bytes)");
    }

    @Override
    public void run ()
    {
        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        super.run(bootstrapServer, topic, consumerGroupId);
    }

    public void run (String bootstrapServer, String topic, String consumerGroupId)
    {
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;

        this.run();
    }
}

