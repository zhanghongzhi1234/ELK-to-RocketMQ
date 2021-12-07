package com.bigdata.springboot.es2rocketmq;

import com.bigdata.springboot.bean.RocketMQConsumerHelper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static java.lang.Thread.sleep;

public class RocketMQConsumerHelperTest {

    private RocketMQConsumerHelper consumerHelper;
    private String nameServer = "192.168.1.104:9876";
    private String groupName = "ElasticSearch";
    private String topic = "TopicTest";

    @BeforeClass
    public void setup(){
        consumerHelper = new RocketMQConsumerHelper(nameServer, groupName);
        consumerHelper.subscribe(topic);
        consumerHelper.registerMessageListener();
    }

    @Test(groups = "groupCorrect")
    public void Start() throws InterruptedException {
        consumerHelper.start();
        while(true){
            sleep(1000);
        }

    }

    @Test(groups = "groupCorrect")
    public void Stop(){
        consumerHelper.unSubscribe(topic);
        consumerHelper.stop();
    }
}
