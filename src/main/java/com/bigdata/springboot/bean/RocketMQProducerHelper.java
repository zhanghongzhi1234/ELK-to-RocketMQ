package com.bigdata.springboot.bean;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

import static org.apache.rocketmq.client.producer.SendStatus.SEND_OK;

public class RocketMQProducerHelper {

    private DefaultMQProducer producer;
    private String nameServer;
    private String groupName;

    public RocketMQProducerHelper(String nameServer, String groupName) {
        this.nameServer = nameServer;
        this.groupName = groupName;
        init(nameServer, groupName);
    }

    private void init(String nameServer, String groupName) {
        producer = new DefaultMQProducer(groupName);
        // Specify name server addresses.
        producer.setNamesrvAddr(nameServer);
    }

    public boolean start() {
        try {
            producer.start();
            producer.setRetryTimesWhenSendAsyncFailed(0);
            return true;
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void stop() {
        producer.shutdown();
    }

    public boolean sendSync(String topic, String tag, String content) {
        //Create a message instance, specifying topic, tag and message body.
        Message msg = null;
        try {
            msg = new Message(topic, tag, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
            if(sendResult.getSendStatus() == SEND_OK)
                return true;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return false;
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } catch (RemotingException e) {
            e.printStackTrace();
            return false;
        } catch (MQBrokerException e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    public boolean sendASync(String topic, String tag, String content) {
        //Create a message instance, specifying topic, tag and message body.
        Message msg = null;
        try {
            msg = new Message(topic, tag, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
            //Call send message to deliver message to one of brokers.
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("send message OK, %s %n", sendResult.getMsgId());
                }
                @Override
                public void onException(Throwable e) {
                    System.out.printf("send message failed, Exception %s %n", e);
                    e.printStackTrace();
                }
            });
            return true;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } catch (RemotingException e) {
            e.printStackTrace();
            return false;
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean sendOneway(String topic, String tag, String content) {
        //Create a message instance, specifying topic, tag and message body.
        Message msg = null;
        try {
            msg = new Message(topic, tag, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
            //Call send message to deliver message to one of brokers.
            producer.sendOneway(msg);
            System.out.printf("send message oneway ok");
        } catch (UnsupportedEncodingException | MQClientException | RemotingException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
