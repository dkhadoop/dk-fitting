package com.dksou.fitting.stream.service.impl;

import com.dksou.fitting.stream.service.DKStreamProducerService;

import com.dksou.fitting.stream.utils.ProducerUtils;
import com.dksou.fitting.stream.utils.Topicutil;
import org.apache.thrift.TException;

import java.util.List;

public class DKStreamProducerServiceImpl implements DKStreamProducerService.Iface {


    public void createTopic(String topicName, int partitions, int replicationFactor) throws TException {
        Topicutil.createTopic(topicName,partitions,replicationFactor);

    }

    public void deleteTopic(String topicName) throws TException {
        Topicutil.deleteTopic(topicName);
    }

    public boolean topicIsExists(String topicName) throws TException {
        boolean b = Topicutil.topicIsExists(topicName);
        return b;
    }

    public List<String> queryAllTopic() throws TException {
        List<String> stringList = Topicutil.queryAllTopic();
        return stringList;
    }

    public void streamDataToTopic(String topicName, String key, String message) throws TException {
        ProducerUtils.sendOne(topicName,key,message);
    }
}
