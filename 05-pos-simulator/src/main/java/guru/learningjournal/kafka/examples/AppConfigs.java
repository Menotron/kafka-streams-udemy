package guru.learningjournal.kafka.examples;

class AppConfigs {
    final static String applicationID = "Multi-Threaded-POS";
    final static String topicName = "pos";
    final static int numThreads = 2;
    final static long producerWaitMs = 1000;
    final static String kafkaConfigFileLocation = "kafka.properties";
}
