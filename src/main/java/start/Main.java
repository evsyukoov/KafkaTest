package start;

import consumer.MyConsumer;
import producer.MyProducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {

        LogManager.getLogManager().readConfiguration(new FileInputStream("./src/main/resources/logging.properties"));

        logger.log(Level.INFO, "Start writing to Kafka..");
        MyProducer client = new MyProducer();
        client.fillKafka();
        logger.log(Level.INFO, "Writing to kafka has been successfully finished. Start reading");
        MyConsumer consumer = new MyConsumer();
        consumer.run();
        logger.log(Level.INFO, "Reading and FizzBuzzing has been successfully finished");

    }
}
