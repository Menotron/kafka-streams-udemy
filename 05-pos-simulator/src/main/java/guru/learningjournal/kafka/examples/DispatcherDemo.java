package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DispatcherDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args){
        Properties props = new Properties();
        try {
            InputStream inputstream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            props.load(inputstream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<>(props);
        Thread[] dispatchers = new Thread[AppConfigs.numThreads];
        logger.info("Starting Dispatcher threads...");
        for(int i = 0; i< AppConfigs.numThreads; i++){
            dispatchers[i] = new Thread(new Dispatcher(producer, AppConfigs.topicName, AppConfigs.producerWaitMs));
            dispatchers[i].start();
        }

        try{
            for (Thread t : dispatchers) t.join();
        }catch (InterruptedException e){
            logger.error("Main Thread Interrupted");
        }finally {
            producer.close();
            logger.info("Finished Dispatcher Demo");
        }


        /*ExecutorService executor = Executors.newFixedThreadPool(AppConfigs.numThreads);
        final List<Dispatcher> runnableProducers = new ArrayList<>();

        for (int i = 0; i < AppConfigs.numThreads; i++) {
            Dispatcher runnableProducer = new Dispatcher(i, producer, AppConfigs.topicName, AppConfigs.producerWaitMs);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Dispatcher p : runnableProducers) p.shutdown();
            executor.shutdown();
            logger.info("Closing Executor Service");
            try {
                executor.awaitTermination(AppConfigs.producerWaitMs * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));*/

    }
}
