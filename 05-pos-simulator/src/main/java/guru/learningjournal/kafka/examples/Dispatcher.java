package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class Dispatcher implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private String topicName;
    private long waitDuration;
    private KafkaProducer<String, PosInvoice> producer;
    private InvoiceGenerator invoiceGenerator;
    private int id;
    private final AtomicBoolean stopper = new AtomicBoolean(false);

    Dispatcher(KafkaProducer<String,PosInvoice> producer, String topicName, long waitDuration){
        this.producer = producer;
        this.topicName = topicName;
        this.waitDuration = waitDuration;
        this.invoiceGenerator = InvoiceGenerator.getInstance();
    }

    Dispatcher(int id, KafkaProducer<String, PosInvoice> producer, String topicName, long waitDuration) {
        this.id = id;
        this.producer = producer;
        this.topicName = topicName;
        this.waitDuration = waitDuration;
        this.invoiceGenerator = InvoiceGenerator.getInstance();
    }

    @Override
    public void run() {
        logger.info("Generating Random Invoice Event...");
        try {
            while(!Thread.currentThread().isInterrupted()) {
                PosInvoice invoice = invoiceGenerator.getNextInvoice();
                logger.info("Sending Invoice to Kafka...");
                producer.send(new ProducerRecord<>(topicName, invoice.getStoreID(), invoice));
                Thread.sleep(waitDuration);
                logger.info("Finished Sending Invoice...");
            }
            logger.info("Starting producer thread - " + id);
            /*while (!stopper.get()) {
                PosInvoice posInvoice = invoiceGenerator.getNextInvoice();
                producer.send(new ProducerRecord<>(topicName, posInvoice.getStoreID(), posInvoice));
                Thread.sleep(waitDuration);
            }*/
        } catch (Exception e) {
            logger.error("Exception in Producer thread ");
            throw new RuntimeException(e);
        }

    }

    /*void shutdown() {
        logger.info("Shutting down producer thread - " + id);
        stopper.set(true);
    }*/
}
