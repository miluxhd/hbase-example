package ir.milux.metalmarks.client;

import com.rabbitmq.client.*;
import ir.milux.metalmarks.core.CONSTANTS;
import ir.milux.metalmarks.core.DB;
import ir.milux.metalmarks.core.MOB;
import org.apache.log4j.Logger;
import java.io.IOException;

public class MOBConsumer {
    private static Logger logger = Logger.getRootLogger();
    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword(CONSTANTS.PASSWORD);
        factory.setUsername(CONSTANTS.USER_NAME);
        factory.setHost(CONSTANTS.RABBIT_HOST);
        Channel inputChannel = null;

        try {
            inputChannel = factory.newConnection().createChannel();
            inputChannel.queueDeclare(CONSTANTS.OUTPUT_QUEUE_NAME,true,false,false,null);
        } catch (Exception e) {
            logger.error(MOBConsumer.class+","+e);
        }
        inputChannel.basicQos(CONSTANTS.RABBIT_QOS);
        Channel finalInputChannel = inputChannel;
        final Consumer consumer = new DefaultConsumer(finalInputChannel) {
            @Override
            public void handleDelivery(String consumerTag,Envelope envelope,AMQP.BasicProperties properties,byte[] body) throws IOException {
                MOB mob = MOB.deserialize(body);
                System.out.println(mob.getName());
                if (DB.put(mob))
                    finalInputChannel.basicAck(envelope.getDeliveryTag(),true);
            }
        };
        boolean autoAck = false;
        inputChannel.basicConsume(CONSTANTS.OUTPUT_QUEUE_NAME,autoAck,consumer);

    }
}

