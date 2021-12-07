package com.bigdata.springboot.es2rocketmq;

import com.bigdata.springboot.bean.RocketMQProducerHelper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RocketMQProducerHelperTest {

    private RocketMQProducerHelper producerHelper;
    private String nameServer = "192.168.1.104:9876";
    private String groupName = "ElasticSearch";
    private String topic = "TopicTest";

    @BeforeClass
    public void setup(){
        producerHelper = new RocketMQProducerHelper(nameServer, groupName);
    }

    @Test(groups = "groupCorrect")
    public void Start(){
        producerHelper.start();
        boolean result = producerHelper.sendSync(topic, "ES", "test12345content");
        Assert.assertTrue(result);
    }

    @Test(groups = "groupCorrect")
    public void Stop(){
        producerHelper.stop();
    }
}
