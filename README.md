# Producer-Consumer Problem using Kafka

This project demonstrates the Producer-Consumer problem using Apache Kafka.

## Prerequisites

Ensure you have Kafka installed on your system. If not, you can download it from the [official Apache Kafka website](https://kafka.apache.org/downloads).

## Steps to Set Up and Run Kafka

### 1. Start Zookeeper

Kafka requires Zookeeper to be running. Use the following command to start Zookeeper:

```sh
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

### 2. Start Kafka Server

After starting Zookeeper, start the Kafka server:

```sh
bin\windows\kafka-server-start.bat config\server.properties
```

### 3. Create a Kafka Topic

Create a topic named `my_first` with the following command:

```sh
bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic my_first
```

## Next Steps

- Run the `main` method to start the Kafka Producer and Consumer.

