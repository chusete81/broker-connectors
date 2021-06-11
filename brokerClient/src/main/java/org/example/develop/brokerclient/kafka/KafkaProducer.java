package org.example.develop.brokerclient.kafka;

import org.example.develop.brokerclient.kafka.connector.KafkaPublisher;
import org.example.develop.brokerclient.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
public class KafkaProducer
{
    public void sendMessages (String bootstrapServer, String topic, int numMessages, int maxMsgSize, int delayMs)
    {
        int count = 0;
        long bytes = 0;
        while (numMessages > count)
        {
            String msg = Utils.generateMsg(maxMsgSize);
            msg = (1 + count++) + "_" + msg;
            msg = msg.substring(0, Math.min(maxMsgSize, msg.length()));

            KafkaPublisher.publish(bootstrapServer, topic, msg);

            bytes += msg.getBytes().length;

            log.info("[" + (count) + "] " + new Date().toString() + " - msgSent: " + msg.substring(0, Math.min(8, msg.length())));

            if (numMessages > count)
            {
                try
                {
                    Thread.sleep(delayMs);
                }
                catch (InterruptedException e)
                {
                    log.error(e.getMessage());
                }
            }
        }

        log.info("Bytes sent: " + bytes);
    }
}
