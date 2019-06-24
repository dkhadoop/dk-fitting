package com.dksou.fitting.stream.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * kafka的producer发送配置文件类
 */
public class ProducerUtils {

    static Logger logger = Logger.getLogger(ProducerUtils.class);
    static Properties providerProp = PropUtils.getProp("provider.properties");
    static Producer<String, String> producer;

    static{
        Properties props = new Properties();
        props.put("bootstrap.servers", providerProp.getProperty("provider.kafka.servers"));
        props.put("key.serializer", providerProp.getProperty("provider.kafka.key.serializer"));
        props.put("value.serializer", providerProp.getProperty("provider.kafka.value.serializer"));
        props.put("acks", providerProp.getProperty("provider.kafka.acks"));
        props.put("retries", providerProp.getProperty("provider.kafka.retries"));
        props.put("batch.size", providerProp.getProperty("provider.kafka.batch.size"));
        props.put("linger.ms", providerProp.getProperty("provider.kafka.linger.ms"));
        props.put("buffer.memory", providerProp.getProperty("provider.kafka.buffer.memory"));
        props.put("max.block.ms", providerProp.getProperty("provider.kafka.max.block.ms"));
        //没有key就是 roundrobin , 有key 就是hash。
        props.put("partitioner.class", providerProp.getProperty("provider.kafka.partitioner.class"));
        props.put("serializer.class", providerProp.getProperty("provider.kafka.serializer.class"));

        /**
         #当消息在producer端沉积的条数达到“queue.buffering.max.messages"后
         #阻塞一定时间后，队列仍然没有enqueue（producer仍然没有发送出任何消息）
         #此时producer可以继续阻塞，或者将消息抛弃
         # -1：无阻塞超时限制，消息不会被抛弃
         # 0 ：立即清空队列，消息被抛弃 queue.enqueue.timeout.ms=-1
         */
        props.put("queue.enqueue.timeout.ms",providerProp.getProperty("provider.kafka.queue.enqueue.timeout.ms"));
        //下面是异步相关的配置。
        props.put("producer.type", providerProp.getProperty("provider.kafka.producer.type"));
        props.put("batch.num.messages", providerProp.getProperty("provider.kafka.batch.num.messages"));
        props.put("queue.buffer.max.ms", providerProp.getProperty("provider.kafka.queue.buffer.max.ms"));
        props.put("queue.buffering.max.messages", providerProp.getProperty("provider.kafka.queue.buffering.max.messages"));

        if( !providerProp.getProperty("provider.kafka.security.protocol").equals("")&& providerProp.getProperty("provider.kafka.security.protocol") != null && !providerProp.getProperty("provider.kafka.ssl.truststore.location").equals("null")){
            props.put("security.protocol", providerProp.getProperty("provider.kafka.security.protocol"));
            props.put("ssl.truststore.location", providerProp.getProperty("provider.kafka.ssl.truststore.location"));
            props.put("ssl.truststore.password", providerProp.getProperty("provider.kafka.ssl.truststore.password"));
            props.put("ssl.keystore.location", providerProp.getProperty("provider.kafka.ssl.keystore.location"));
            props.put("ssl.keystore.password", providerProp.getProperty("provider.kafka.ssl.keystore.password"));
            props.put("ssl.key.password", providerProp.getProperty("provider.kafka.ssl.key.password"));
        }






            producer = new KafkaProducer<String, String>(props);
    }

    /**
     * 发送一条数据到指定topic中。
     * @param topicName topic的名称
     * @param key  键值 。 没有则填null
     * @param message 要发送的消息
     */
    public static void sendOne(String topicName,String key,String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key,message);
        producer.send(record);
        producer.flush();//使用此方法数据会及时的发送...如果不使用数据会在缓存里面等待一起发送到broker中
    }

    public static void main(String[] args) {

        for (int i = 5; i<7; i++){
//            producer.send(new ProducerRecord<String, String>("my-topic",Integer.toString(i),Integer.toString(i)));
//            producer.flush();
            sendOne("my-topic",Integer.toString(i),Integer.toString(i));

        }


    }



}
