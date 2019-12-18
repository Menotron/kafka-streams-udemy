package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class MdEventProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args){
        logger.info("Creating kafka event producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(props);

        logger.info("Starting event generation");
        for(int i=0; i<=AppConfigs.numEvents; i++){
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Event no: " + i));
        }

        logger.info("Finished sending events to kafka. Closing Producer");
        producer.close();
    }
}
