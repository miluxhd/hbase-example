package ir.milux.metalmarks.client;

import com.rabbitmq.client.*;
import ir.milux.metalmarks.core.CONSTANTS;
import ir.milux.metalmarks.core.MOB;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MOBFeeder {
    private static Logger logger = Logger.getRootLogger();
    public static void main(String[] args) throws IOException, InterruptedException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword(CONSTANTS.PASSWORD);
        factory.setUsername(CONSTANTS.USER_NAME);
        factory.setHost(CONSTANTS.RABBIT_HOST);
        Channel inputChannel = null;
        Channel outputChannel = null;
        try {
            inputChannel = factory.newConnection().createChannel();
            outputChannel = factory.newConnection().createChannel();

            inputChannel.queueDeclare(CONSTANTS.INPUT_QUEUE_NAME, true, false, false, null);
            outputChannel.queueDeclare(CONSTANTS.OUTPUT_QUEUE_NAME, true, false, false, null);
        } catch (Exception e) {
            logger.error(MOBFeeder.class + "," + e);
        }
        inputChannel.basicQos(1000);
        Channel finalInputChannel = inputChannel;
        Channel finalOutputChannel = outputChannel;
        final Consumer consumer = new DefaultConsumer(finalInputChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String filename = new String(body, "UTF-8");
                try {
                    byte[] bytes = Files.readAllBytes(Paths.get(filename));
                    MOB mob = new MOB(filename, bytes, Files.probeContentType(Paths.get(filename)));
                    finalOutputChannel.basicPublish("", CONSTANTS.OUTPUT_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, mob.serialize());
                    finalInputChannel.basicAck(envelope.getDeliveryTag(), false);
                } catch (IOException e) {
                    logger.error(MOBFeeder.class + "," + e);
                    finalInputChannel.basicNack(envelope.getDeliveryTag(), false , true);
                }
            }
        };
        boolean autoAck = false;
        inputChannel.basicConsume(CONSTANTS.INPUT_QUEUE_NAME, autoAck, consumer);

    }
}

