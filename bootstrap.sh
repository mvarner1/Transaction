
cd $KAFKA_HOME

# Delete All
sh bin/kafka-topics.sh --delete --zookeeper localhost:32181 --topic transaction
sh bin/kafka-topics.sh --delete --zookeeper localhost:32181 --topic rewards
sh bin/kafka-topics.sh --delete --zookeeper localhost:32181 --topic purchase-patterns
sh bin/kafka-topics.sh --delete --zookeeper localhost:32181 --topic purchase-storage

# Create All
sh bin/kafka-topics.sh --create --zookeeper localhost:32181 --replication-factor 1 --partitions 1 --topic transaction
sh bin/kafka-topics.sh --create --zookeeper localhost:32181 --replication-factor 1 --partitions 1 --topic rewards
sh bin/kafka-topics.sh --create --zookeeper localhost:32181 --replication-factor 1 --partitions 1 --topic purchase-patterns
sh bin/kafka-topics.sh --create --zookeeper localhost:32181 --replication-factor 1 --partitions 1 --topic purchase-storage
