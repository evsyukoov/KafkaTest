package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import start.KafkaConstants;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyConsumer {

    final Logger logger = Logger.getLogger(MyConsumer.class.getName());
    public void run() {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConstants.SERVER_ADDRESS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put("group.id", "test");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer
                <>(props);
        consumer.subscribe(Arrays.asList(KafkaConstants.KAFKA_TOPIC));
        AtomicInteger res = new AtomicInteger();
        while (true) {
            final ConsumerRecords<Integer, String> consumerRecords =
                    consumer.poll(300);

            if (consumerRecords.count() == 0) {
                break;
            }
            consumerRecords.forEach(record -> {
                FizzBuzzHelper.print(record.value());
                res.getAndIncrement();
            });;
            consumer.commitAsync();
        }
        logger.log(Level.INFO, String.format("%d numbers was FizzBuzzed!\n", res.get()));
        consumer.close();
    }

}
