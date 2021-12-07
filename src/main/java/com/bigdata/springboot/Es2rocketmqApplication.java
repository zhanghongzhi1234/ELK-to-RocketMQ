package com.bigdata.springboot;

import com.bigdata.springboot.service.Es2rocketMQService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Es2rocketmqApplication implements CommandLineRunner {

    @Value("${esserver.host}")
    private String esserver_host;

    @Value("${esserver.port1}")
    private int esserver_port1;

    @Value("${esserver.port2}")
    private int esserver_port2;

    @Value("${esserver.pollinterval}")
    private int esserver_pollinterval;

    @Value("${esserver.index}")
    private String esserver_index;

    @Value("${rocketmq.nameserver}")
    private String rocketmq_nameserver;

    @Value("${rocketmq.producergroup}")
    private String rocketmq_producergroup;

    @Value("${rocketmq.topic}")
    private String rocketmq_topic;

    public static void main(String[] args) {
        SpringApplication.run(Es2rocketmqApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println(esserver_host);
        Es2rocketMQService service = new Es2rocketMQService(rocketmq_nameserver, rocketmq_producergroup,
                esserver_host, esserver_port1, esserver_port2, esserver_pollinterval,
                rocketmq_topic, esserver_index);
        service.start();
    }
}
