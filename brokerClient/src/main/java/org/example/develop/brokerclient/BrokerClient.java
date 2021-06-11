package org.example.develop.brokerclient;

import org.example.develop.brokerclient.kafka.KafkaConsumer;
import org.example.develop.brokerclient.kafka.KafkaProducer;
import org.example.develop.brokerclient.rabbitmq.RabbitConsumer;
import org.example.develop.brokerclient.rabbitmq.RabbitProducer;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BrokerClient
{
    private static final String PNAME_BROKER_TYPE = "-brokerType";
    private static final String PNAME_BROKER_HOST = "-host";
    private static final String PNAME_BROKER_PORT = "-port";
    private static final String PNAME_TOPIC = "-topic";
    private static final String PNAME_CLIENT_MODE = "-mode";
    private static final String PNAME_CONSUMER_GROUP = "-consumerGroup";
    private static final String PNAME_NUM_MESSAGES = "-numMessages";
    private static final String PNAME_MESSAGE_SIZE = "-messageSize";
    private static final String PNAME_DELAY_MS = "-delayMs";
    private static final String PNAME_CREDENTIALS = "-login";

    public static void main (String[] args) throws Exception
    {
        Map<String, String> config = getConfigFromParameters(args);

        BrokerType brokerType = BrokerType.valueOf(config.get(PNAME_BROKER_TYPE).toUpperCase());
        String brokerHost = config.get(PNAME_BROKER_HOST);
        int brokerPort = Integer.parseInt(config.get(PNAME_BROKER_PORT));
        String topicOrExchange = config.get(PNAME_TOPIC);
        ClientMode clientMode = ClientMode.valueOf(config.get(PNAME_CLIENT_MODE).toUpperCase());

        if (ClientMode.PRODUCER.equals(clientMode))
        {
            Date before = new Date();

            int numMessages = Integer.parseInt(config.get(PNAME_NUM_MESSAGES));
            int messageSize = Integer.parseInt(config.get(PNAME_MESSAGE_SIZE));
            int delayMs = Integer.parseInt(config.get(PNAME_DELAY_MS));

            if (BrokerType.KAFKA.equals(brokerType))
            {
                new KafkaProducer().sendMessages(brokerHost + ":" + brokerPort, topicOrExchange, numMessages, messageSize, delayMs);
            }
            else if (BrokerType.RABBITMQ.equals(brokerType))
            {
                new RabbitProducer().sendMessages(
                        config.get(PNAME_BROKER_HOST), Integer.parseInt(config.get(PNAME_BROKER_PORT)), config.get(PNAME_TOPIC),
                        Integer.parseInt(config.get(PNAME_NUM_MESSAGES)), Integer.parseInt(config.get(PNAME_MESSAGE_SIZE)), Integer.parseInt(config.get(PNAME_DELAY_MS)), config.get(PNAME_CREDENTIALS));
            }

            Date after = new Date();
            long elapsed = after.getTime() - before.getTime();
            log.info("Elapsed time: {} secs.", elapsed / 1000);

            System.exit(0);
        }
        else if (ClientMode.CONSUMER.equals(clientMode))
        {
            String kfkConsumerGroup = config.get(PNAME_CONSUMER_GROUP);

            if (BrokerType.KAFKA.equals(brokerType))
            {
                new KafkaConsumer().run(brokerHost + ":" + brokerPort, topicOrExchange, kfkConsumerGroup);
            }
            else if (BrokerType.RABBITMQ.equals(brokerType))
            {
                new RabbitConsumer().run(
                        config.get(PNAME_BROKER_HOST), Integer.parseInt(config.get(PNAME_BROKER_PORT)),
                        config.get(PNAME_TOPIC), config.get(PNAME_CONSUMER_GROUP), Integer.parseInt(config.get(PNAME_DELAY_MS)), config.get(PNAME_CREDENTIALS));
            }
        }


    }

    private static Map<String, String> getDefaultConfig ()
    {
        Map<String, String> config = new HashMap<>();

        config.put(PNAME_BROKER_TYPE, "RABBITMQ");
        config.put(PNAME_BROKER_HOST, "localhost");
        config.put(PNAME_BROKER_PORT, "5672");
        config.put(PNAME_TOPIC, "testTopic");
        config.put(PNAME_CLIENT_MODE, "CONSUMER");
        config.put(PNAME_CONSUMER_GROUP, "consumerGroup");
        config.put(PNAME_NUM_MESSAGES, "1");
        config.put(PNAME_MESSAGE_SIZE, "1024");
        config.put(PNAME_DELAY_MS, "0");
        config.put(PNAME_CREDENTIALS, "user:pass");

        return config;
    }

    private static Map<String, String> getConfigFromParameters (String[] args)
    {
        Map<String, String> config = getDefaultConfig();

        Arrays.stream(args).forEach(arg -> {
            if (arg != null && arg.contains("="))
            {
                String[] param = arg.split("=");

                if (config.containsKey(param[0]) && param[1].length() > 0)
                {
                    config.put(param[0], param[1]);
                }
            }
        });

        StringBuilder sb = new StringBuilder("BrokerClient values:\n");
        config.keySet().forEach(s -> sb.append(String.format("        %s = %s\n", s.substring(1, s.length()), config.get(s))));
        log.info(sb.toString());

        return config;
    }

    public enum BrokerType
    {
        KAFKA, RABBITMQ;
    }

    public enum ClientMode
    {
        PRODUCER, CONSUMER;
    }
}
