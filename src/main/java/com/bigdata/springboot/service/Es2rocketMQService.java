package com.bigdata.springboot.service;

import com.bigdata.springboot.bean.ElasticSearchHelper;
import com.bigdata.springboot.bean.RocketMQProducerHelper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Timer;

@Component
public class Es2rocketMQService {

    private RocketMQProducerHelper producerHelper = null;

    private ElasticSearchHelper esHelper = null;

    private Es2rocketMQTask task = null;
    Timer timer = new Timer();
    int essserver_pollinverval;

    public Es2rocketMQService(String rocketmq_nameserver, String rocketmq_producergroup,
                              String esserver_host, int esserver_port1, int esserver_port2, int essserver_pollinverval,
                              String rocketmq_topic, String esserver_index) {
        try {
            producerHelper = new RocketMQProducerHelper(rocketmq_nameserver, rocketmq_producergroup);
            if(producerHelper == null){
                System.out.println("Cannot open create rocketMQ producer");
            }

            esHelper = new ElasticSearchHelper(esserver_host, esserver_port1, esserver_port2);
            if(esHelper == null){
                System.out.println("Cannot open create elastic search helper");
            }

            task = new Es2rocketMQTask(producerHelper, esHelper, rocketmq_topic, esserver_index);
            this.essserver_pollinverval = essserver_pollinverval;
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    public void start(){
        producerHelper.start();
        timer.schedule(task, 2000L, essserver_pollinverval);
    }

    public void stop(){
        timer.cancel();
        producerHelper.stop();
    }
}
