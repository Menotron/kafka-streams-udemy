kafka-topics.bat --create --zookeeper localhost:2181 --topic hello-producer-topic1 --partitions 5 --replication-factor 3 --config min.insync.replicas=2