package com.sensorsdata.analytics.javasdk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * class_name: KafkaProducerHandler
 * package: com.sensorsdata.analytics.javasdk.util
 * describe: 多线程kafka生产者
 * author: cocopc
 * email: m18311283082@163.com
 * date: 2017/6/12
 **/
public class KafkaProducerHandler implements Runnable {
    private final static Logger log = LoggerFactory.getLogger(KafkaProducerHandler.class);

    private String message;
    private String broker;
    private String topic;
    private Properties props;
    private int retry;

    public KafkaProducerHandler(String broker, String topic, Properties props, int retry, String message) {
        this.message = message;
        this.broker = broker;
        this.topic = topic;
        this.props = props;
        this.retry = retry;
    }

    @Override
    public void run() {
        KafkaProducer kafkaProducer = KafkaProducer.get();
        kafkaProducer.init(broker, topic, props, retry);
        log.info(String
                .format("Producer 当前线程 %s ,Kafka 实例 %s ", Thread.currentThread().getName(), kafkaProducer.toString()));
        kafkaProducer.send(message);
    }
}