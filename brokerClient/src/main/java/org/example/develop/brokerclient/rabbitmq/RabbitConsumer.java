package org.example.develop.brokerclient.rabbitmq;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.example.develop.brokerclient.utils.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RabbitConsumer
{
    public void run (String host, int port, String topic, String consumerGroup, int delayMs, String credentials) throws Exception
    {
        String exchangeName = "exch_" + topic;
        String queueName = "q_" + topic + "_" + consumerGroup;
        boolean durable = true;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(Utils.getUser(credentials));
        factory.setPassword(Utils.getPassword(credentials));

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        try
        {
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, durable);
            channel.queueDeclare(queueName, durable, false, false, null);
            channel.queueBind(queueName, exchangeName, "");
        }
        catch (IOException e)
        {
            log.warn("Error trying to declare exchange, queue or bind. Maybe they already exist?");
            channel = connection.createChannel();
        }

        AtomicInteger numMessages = new AtomicInteger(0);

        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);

            log.info("[" + (numMessages.incrementAndGet()) +"] " + new Date().toString() + " - msgRcvd: " +
                    msg.substring(0, Math.min(8, msg.length())) + " (" + msg.length() + " bytes)");

            try
            {
                Thread.sleep(delayMs);
            }
            catch (InterruptedException e)
            {
                log.error(e.getMessage());
            }
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
