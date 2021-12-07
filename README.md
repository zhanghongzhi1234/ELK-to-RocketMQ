# ELK-to-RocketMQ
ElasticSearch data streaming to RocketMQ

# Background
Use rest API to maintain the Streaming job.  
User RocketMQ to send the data frame(from ElasticsSearch) to client-side.  
It's pub-sub model, use "Topic" concept to differentiate the job.  

# Requirement
The communication in between ELK to Kubeflow consists of  
1. Control plane, command -receining, e.g. start streaming, stop, and pause.
2. Data Plane, for dataframe streaming,

Sequence steps:
1. User control plane to send the command to container.  
    Herein, normally, the container has rocketMQ client to send sniff to server:port to get proper command and data:topic to pull data from.
2. Once the rockeMQ client in container has got the command to pull msg in corresponding topic.  
   Start the threads to pull data frame.
3. RokcerMQ client continue to listen the port from rockerMQ, for further actions, e.g. stop, pause...

Notes:
1. Use rocketMQ as message pub-sub service
2. Streaming CRUD service ----> client in model/training container

![](./docs/images/rocketMQ.png)

# Analysis
RocketMQ is Alibaba's open source distributed messaging middleware, which was later contributed to apache, now called Apache RocketMQ.

ActiveMQ, RocketMQ, RabbitMQ, Kafka are all message queues  

Features | ActiveMQ | RabbitMQ | RocketMQ | kafka
---- | ---- | ---- |---- | ----
Development language | java | erlang | java | scala
Single machine throughput | 10,000 level | 10,000 level | 100,000 level | 100,000 level
Timeliness | ms level | us level | ms level | within ms level
Availability | High (master-slave architecture) | High (master-slave architecture) | Very high (distributed architecture) | Very high (distributed architecture)
Functional characteristics | Mature products, which are used in many companies; there are more documents; various protocol support is better. | Based on erlang development, so the concurrency is very strong, the performance is extremely good, and the delay is very low; the management interface is richer. | MQ function comparison Complete and good scalability. | Only the main MQ functions are supported. Some functions such as message query and message backtracking are not provided. After all, they are prepared for big data and are widely used in the field of big data.

RocketMQ has a producer group, but it is of no special use. It is the redundancy of sending the same message. Only when producerA goes down, the transaction message is always in the PREPARED state and timed out, then the broker will check back to other producers in the same group to confirm this Should the message be commit or rollback. But the open source version does not support transaction messages.  

The tag of rocketMQ is an extension of kafka topic, and it can be filtered by tag when receiving
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_EXAMPLE");
consumer.subscribe("TOPIC", "TAGA || TAGB || TAGC");  
If you specify tagA when you subscribe, then tagB messages will not be delivered  

Reocket have 3 ways for sending message:
1) sendSync
2) sendAsync
3) sendOneway: this method won't wait for acknowledgement from broker before return. Obviously, it has maximum throughput yet potentials of message loss

# RocketMQ Installation and Command Set:

Download & Build from Release
  > unzip rocketmq-all-4.7.0-source-release.zip
  > cd rocketmq-all-4.7.0/
  > mvn -Prelease-all -DskipTests clean install -U
  > cd distribution/target/rocketmq-4.7.0/rocketmq-4.7.0

Start Name Server
  > nohup sh bin/mqnamesrv &
  > tail -f ~/logs/rocketmqlogs/namesrv.log
  The Name Server boot success...

Start Broker
  > nohup sh bin/mqbroker -n localhost:9876 &
  > tail -f ~/logs/rocketmqlogs/broker.log 
  The broker[%s, 172.30.30.233:10911] boot success...

1. Send & Receive Messages
Before sending/receiving messages, we need to tell clients the location of name servers. RocketMQ provides multiple ways to achieve this. For simplicity, we use environment variable NAMESRV_ADDR

 > export NAMESRV_ADDR=localhost:9876
 > sh bin/tools.sh org.apache.rocketmq.example.quickstart.Producer
 SendResult [sendStatus=SEND_OK, msgId= ...

 > sh bin/tools.sh org.apache.rocketmq.example.quickstart.Consumer
 ConsumeMessageThread_%d Receive New Messages: [MessageExt...

2. Shutdown Servers
> sh bin/mqshutdown broker
The mqbroker(36695) is running...
Send shutdown request to mqbroker(36695) OK

> sh bin/mqshutdown namesrv
The mqnamesrv(36664) is running...
Send shutdown request to mqnamesrv(36664) OK

3. View all consumer groups:  
   sh mqadmin consumerProgress -n 192.168.1.23:9876
4. View the accumulation of all topic data under the specified consumer group:  
    sh mqadmin consumerProgress -n 192.168.1.23:9876 -g warning-group
5. View all topics:  
     sh mqadmin topicList -n 192.168.1.23:9876
6. View detailed statistics of topic information list:  
   sh mqadmin topicstatus -n 192.168.1.23:9876 -t topicWarning
7.  Add topic:  
   sh mqadmin updateTopic –n 192.168.1.23:9876 –c DefaultCluster –t topicWarning
8. Delete topic:  
  sh mqadmin deleteTopic –n 192.168.1.23:9876 –c DefaultCluster –t topicWarning
 
9. Query cluster information:
sh mqadmin  clusterList -n 192.168.1.23:9876

# Folder Structure
+ bean: 
    - ElasticSearchHelper.java   
    ElasticSearch rest client, implement index create/delete, data insert/delete, ES start/stop etc...
    - RocketMQConsumerHelper.java  
    RocketMQ Consumer, subscribe / unsub topic, Callback when receive message
    - RocketMQProducer.java
    RocketMQ Producer, send message sync/async/oneway
+ service:
    - Es2rocketMQService.java  
    Create thread task to streaming data from ES to RocketMQT
    - Es2rocketMQTask.java  
    Task implementation for Poll ElasticSearch by configed index, then send to configed RocketMQ topic.

# Issues:
1. RocketMQ latest version 4.7.0 have bug, when use   
#./mqadmin topicList -n 127.0.0.1:9876  
to see all topic list, it pop error, change back to 4.3.0, no such error

## License

This appplication is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details..