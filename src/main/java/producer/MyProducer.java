package producer;

import consumer.MyConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import start.KafkaConstants;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyProducer {

    final private static Logger logger = Logger.getLogger(MyProducer.class.getName());

    final private static int START = 1;

    final private static int FINISH = 1000000;

    Producer<Integer, String> producer;

    Properties properties;

    public MyProducer() {
        this.properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConstants.SERVER_ADDRESS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put("timeout.ms", "50");
        properties.put("transactional.id", "1");
        this.producer = new KafkaProducer<>(properties, new IntegerSerializer(), new StringSerializer());

    }

    public void fillKafka() {
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (int i = START; i <= FINISH; i++) {
                producer.send(new ProducerRecord<>(KafkaConstants.KAFKA_TOPIC, i, String.valueOf(i)));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            logger.log(Level.SEVERE, "Failed write to kafka: ", e.getCause());
            producer.close();
        } catch (KafkaException e) {
            logger.log(Level.SEVERE, "Abort transaction: ", e.getCause());
            producer.abortTransaction();
        }
        producer.close();
    }
}
