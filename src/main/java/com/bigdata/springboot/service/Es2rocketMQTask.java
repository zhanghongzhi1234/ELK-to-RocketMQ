package com.bigdata.springboot.service;

import com.bigdata.springboot.bean.ElasticSearchHelper;
import com.bigdata.springboot.bean.RocketMQProducerHelper;

import java.util.List;
import java.util.TimerTask;

public class Es2rocketMQTask extends TimerTask {

    private RocketMQProducerHelper producerHelper = null;
    private ElasticSearchHelper esHelper = null;
    private String topic;
    private String index;

    public Es2rocketMQTask(RocketMQProducerHelper producerHelper, ElasticSearchHelper esHelper, String topic, String index) {
        this.producerHelper = producerHelper;
        this.esHelper = esHelper;
        this.topic = topic;
        this.index = index;
    }

    @Override
    public void run() {
        System.out.println("Poll ElasticSearch, index = " + index);
        List<String> dataList = esHelper.GetDataBySearch(index);
        if(dataList != null && dataList.size() > 0) {
            Boolean result = producerHelper.sendASync(topic, "ElasticSearch", String.join(",", dataList));
            if(result == true)
                System.out.println("send data to rocketMQ successfully");
            else
                System.out.println("send data to rocketMQ failed");
        }
        else
            System.out.println("No data polled from ElasticSearch");
    }
}

