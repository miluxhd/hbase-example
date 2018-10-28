package ir.milux.metalmarks.client;


import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import ir.milux.metalmarks.core.CONSTANTS;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class AddressFeeder {
    private static Logger logger = Logger.getRootLogger();
    public static void main(String[] args) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword(CONSTANTS.PASSWORD);
        factory.setUsername(CONSTANTS.USER_NAME);
        factory.setHost(CONSTANTS.RABBIT_HOST);
        Channel channel = null;

        try {
            channel = factory.newConnection().createChannel();
            channel.queueDeclare(CONSTANTS.INPUT_QUEUE_NAME, true, false, false, null);
        }catch (Exception e){
            logger.error(AddressFeeder.class+","+e);
        }
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(CONSTANTS.INPUT_FILE_LIST));
        while ((line = reader.readLine())!=null){
            System.out.println(line);;
            channel.basicPublish("", CONSTANTS.INPUT_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, line.getBytes());
        }

        try {
            channel.close();
        } catch (TimeoutException e) {
            logger.error(e);
        }
    }
}

