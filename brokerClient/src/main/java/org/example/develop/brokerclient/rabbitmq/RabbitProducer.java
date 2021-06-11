package org.example.develop.brokerclient.rabbitmq;

import org.example.develop.brokerclient.utils.Utils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitProducer
{
    public void sendMessages (String host, int port, String queueName, int numMessages, int maxMsgSize, int delayMs, String credentials)
    {
        Channel channel = getChannel(host, port, credentials);

        int count = 0;
        long bytes = 0;
        while (numMessages > count)
        {
            try
            {
                String msg = Utils.generateMsg(maxMsgSize);
                msg = (1 + count++) + "_" + msg;
                msg = msg.substring(0, Math.min(maxMsgSize, msg.length()));

                channel.basicPublish("exch_" + queueName, "", MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes());

                bytes += msg.getBytes().length;

                log.info("[" + (count) + "] " + new Date().toString() + " - msgSent: " + msg.substring(0, Math.min(8, msg.length())));

                if (numMessages > count)
                {
                    Thread.sleep(delayMs);
                }
            }
            catch (IOException | InterruptedException e)
            {
                log.error(e.getMessage());
            }
        }

        log.info("Bytes sent: " + bytes);
    }

    private static Channel getChannel (String host, int port, String credentials)
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(Utils.getUser(credentials));
        factory.setPassword(Utils.getPassword(credentials));

        Channel channel = null;
        try
        {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
        }
        catch (IOException | TimeoutException e)
        {
            log.error(e.getMessage());
        }

        return channel;
    }
}
