package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Dispatcher implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private String topicName;
    private long wait_ms;
    private KafkaProducer<Integer, PosInvoice> producer;
    private InvoiceGenerator igen = InvoiceGenerator.getInstance();

    Dispatcher(KafkaProducer<Integer,PosInvoice> producer, String topicName, long wait_ms){
        this.producer = producer;
        this.topicName = topicName;
        this.wait_ms = wait_ms;
    }

    @Override
    public void run() {
        logger.info("Generating Random Invoice Event...");
        try {
            PosInvoice invoice = igen.getNextInvoice();
            logger.info("Sending Invoice to Kafka...");
            producer.send(new ProducerRecord<>(topicName,null, invoice));
            Thread.sleep(wait_ms);
            logger.info("Finished Sending Invoice...");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
