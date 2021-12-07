package com.bigdata.springboot.bean;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class RocketMQConsumerHelper {

    private DefaultMQPushConsumer consumer;
    private String nameServer;
    private String groupName;

    public RocketMQConsumerHelper(String nameServer, String groupName) {
        this.nameServer = nameServer;
        this.groupName = groupName;
        init(nameServer, groupName);
    }

    private void init(String nameServer, String groupName) {
        consumer = new DefaultMQPushConsumer(groupName);
        // Specify name server addresses.
        consumer.setNamesrvAddr(nameServer);
        //consumer.setPullInterval(5 * 1000);
    }

    public boolean subscribe(String topic) {
        // Subscribe one more more topics to consume.
        try {
            consumer.subscribe(topic, "*");
            return true;
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean subscribe(String topic, String filter) {
        // Subscribe one more more topics to consume.
        try {
            consumer.subscribe(topic, filter);
            return true;
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void unSubscribe(String topic) {
        consumer.unsubscribe(topic);
    }

    // Register callback to execute on arrival of messages fetched from brokers.
    public void registerMessageListener(){
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
    }

    public boolean start() {
        try {
            consumer.start();
            System.out.printf("Consumer Started.%n");
            return true;
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void stop() {
        consumer.shutdown();
    }

}
