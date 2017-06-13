package com.sensorsdata.analytics.javasdk.util;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * class_name: KafkaProducer
 * package: com.sensorsdata.analytics.javasdk.util
 * describe: kafka producer 是线程安全的，单例形式封装kafka生产者，多线程共享一个实例
 * author: cocopc
 * email: m18311283082@163.com
 * date: 2017/6/9
 **/
public class KafkaProducer {

    private final static Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private static org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private int retry;

    private KafkaProducer() {
    }

    private static class LazyHandler {
        private static final KafkaProducer INSTANCE = new KafkaProducer();
    }

    public static final KafkaProducer get() {
        return LazyHandler.INSTANCE;
    }

    /**
     * @param broker kafka server，可以是host:port,...,host:port
     * @param topic  kafka topic
     * @param prop   kafka 生产者的配置属性，传入的属性会覆盖默认的值
     *               author cocopc
     *               date 2017/6/9
     */
    public void init(String broker, String topic, Properties prop, int retry) {
        this.topic = topic;
        this.retry = retry;
        if (null == kafkaProducer) {
            Properties props = new Properties();
            props.put("bootstrap.servers", broker);
            props.put("acks", "1");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("max.block.ms", 30000);
            props.put("max.request.size",10485760);

            if (prop != null) {
                props.putAll(prop);
            }

            kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        }


    }

    /**
     * @param message 要发送到kafka的消息
     *                author cocopc
     *                date 2017/6/9
     */
    public void send(final String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                topic, message);
        // send方法是异步的,添加消息到缓存区等待发送,并立即返回，这使生产者通过批量发送消息来提高效率
        // kafka生产者是线程安全的,可以单实例发送消息

        try{
        kafkaProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata,
                                     Exception exception) {
                if (null != exception) {
                    log.error(String.format("kafka发送消息失败,message:%s", message), exception);
                    retrySend(message);
                } else {
                    log.info(String.format("The offset of the record we just sent is:%s", recordMetadata.offset()));
                }
            }
        });
        }catch (Exception e){
            log.error(String.format("kafka发送消息失败,message:%s", message), e);
        }
    }


    /**
     * 当kafka消息发送失败后,重试发送，这里重试发送时，采用同步发送，经过重试后如果还是不能发送消息，则打印消息日志
     *
     * @param message
     */
    private void retrySend(final String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                topic, message);
        for (int i = 1; i <= retry; i++) {
            try {
                kafkaProducer.send(record).get();
                return;
            } catch (Exception e) {
                log.error(String.format("第 %s 次重试发送 kafka 消息失败,message:%s", i, message), e);
            }
        }
    }

    public void close() {

        if (null != kafkaProducer) {
            kafkaProducer.close();
            kafkaProducer=null;
        }

    }

}
