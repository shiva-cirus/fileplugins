



###Getting Started with Kafka
==========================

Kafka Local Setup Instructions

Ensure that you have Java JDK 8 running and installed on your Mac

Download and Kafka latest binary kafka_2.11-2.3.1

There are a bunch of processes that we need to start to run our cluster:
Zookeeper: Used to maintain states between Kafka nodes ensemble
Kafka Brokers: Used to Store and emit data i.e the message queue
Producers: To push data to the cluster
Consumers: read data from the cluster (this is optional but for testing in our case)




Starting Zookeeper
Go to the downloaded Kafka directory “kafka_2.11-2.3.1”
Run `bin/zookeeper-server-start.sh config/zookeeper.properties`

You should see the following output, everything is ran successfully

[2019-11-03 16:15:39,355] INFO Server environment:user.dir=/Users/mavencode/development/mavencode2019/CirrusApp/kafka_2.11-2.3.1 (org.apache.zookeeper.server.ZooKeeperServer)
[2019-11-03 16:15:39,369] INFO tickTime set to 3000 (org.apache.zookeeper.server.ZooKeeperServer)
[2019-11-03 16:15:39,369] INFO minSessionTimeout set to -1 (org.apache.zookeeper.server.ZooKeeperServer)
[2019-11-03 16:15:39,369] INFO maxSessionTimeout set to -1 (org.apache.zookeeper.server.ZooKeeperServer)
[2019-11-03 16:15:39,389] INFO Using org.apache.zookeeper.server.NIOServerCnxnFactory as server connection factory (org.apache.zookeeper.server.ServerCnxnFactory)
[2019-11-03 16:15:39,411] INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)

Starting Brokers
We want to run a 3 broker Kafka node so we need to modify the  `config/server.properties`
 With the following values
 ```
config/server.0.properties
broker.id=0
listeners=PLAINTEXT://:9092
log.dirs=/tmp/kafka-logs-0
config/server.1.properties
broker.id=1
listeners=PLAINTEXT://:9093
log.dirs=/tmp/kafka-logs-1
config/server.2.properties
broker.id=2
listeners=PLAINTEXT://:9094
log.dirs=/tmp/kafka-logs-2
```

make dir for the log files
```
/tmp/kafka-logs-0, /tmp/kafka-logs-1, /tmp/kafka-logs-2
```

Start the broker instances
```bin/kafka-server-start.sh config server.1.properties
bin/kafka-server-start.sh config server.2.properties
bin/kafka-server-start.sh config server.3.properties
```

If everything is ok you should the brokers joining the Zookeeper cluster

```
[2019-11-03 16:31:15,715] INFO [TransactionCoordinator id=0] Starting up. (kafka.coordinator.transaction.TransactionCoordinator)
[2019-11-03 16:31:15,717] INFO [Transaction Marker Channel Manager 0]: Starting (kafka.coordinator.transaction.TransactionMarkerChannelManager)
[2019-11-03 16:31:15,717] INFO [TransactionCoordinator id=0] Startup complete. (kafka.coordinator.transaction.TransactionCoordinator)
[2019-11-03 16:31:15,775] INFO [/config/changes-event-process-thread]: Starting (kafka.common.ZkNodeChangeNotificationListener$ChangeEventProcessThread)
[2019-11-03 16:31:15,789] INFO [SocketServer brokerId=0] Started data-plane processors for 1 acceptors (kafka.network.SocketServer)
[2019-11-03 16:31:15,793] INFO Kafka version: 2.3.1 (org.apache.kafka.common.utils.AppInfoParser)
[2019-11-03 16:31:15,793] INFO Kafka commitId: 18a913733fb71c01 (org.apache.kafka.common.utils.AppInfoParser)
[2019-11-03 16:31:15,793] INFO Kafka startTimeMs: 1572820275789 (org.apache.kafka.common.utils.AppInfoParser)
[2019-11-03 16:31:15,796] INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
```

	On the zookeeper, you should see the broker instance joining with id=0,1 and 2

Create a Topic

Run -> 
`bin/kafka-topics.sh --create --topic cdap-topic --zookeeper localhost:2181 --partitions 3 --replication-factor 2
`

	
Start a Producer that will connect to `cdap-topic`	

`bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --topic cdap-topic
`

Run a Test Consumer to ensure messages delivered to the Topic from the producer are been read



#Getting Started with CDAP
==========================
Download CDAP Sandbox -> https://docs.cask.co/cdap/5.0.0/en/developer-manual/getting-started/sandbox/zip.html

Unzip the folder
cd `cdap-sandbox-6.0.0`
Run ` bin/cdap sandbox start` to start the CDAP environment


Configure The Source with Kafka Consumer and Output the result to a SnapshotText

Preview Run to make sure the result match with the data coming from the simulated producer




