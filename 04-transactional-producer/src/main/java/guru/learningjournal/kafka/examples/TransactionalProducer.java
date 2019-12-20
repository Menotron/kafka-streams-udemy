package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class TransactionalProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args){
        logger.info("Creating kafka event producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,AppConfigs.transaction_id);

        KafkaProducer<Integer,String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        logger.info("Starting First Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 0; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Service1-T1 Event no: " + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Service2-T1 Event no: " + i));
            }
            logger.info("Committing First Transaction...");
            producer.commitTransaction();
        }catch(Exception e){
            logger.error("Exception in First Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Starting Second Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 0; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Service1-T2 Event no: " + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Service2-T2 Event no: " + i));
                throw new IllegalArgumentException();
            }
            logger.info("Committing Second Transaction...");
            producer.commitTransaction();
        }catch(Exception e){
            logger.error("Exception in Second Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Finished sending events to kafka. Closing Producer");
        producer.close();
    }
}
