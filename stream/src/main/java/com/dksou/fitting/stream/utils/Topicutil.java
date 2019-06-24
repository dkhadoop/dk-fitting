package com.dksou.fitting.stream.utils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;

import java.util.*;

public class Topicutil {


    static Properties providerProp = PropUtils.getProp("provider.properties");

    static String zkUrl;
    static int sessionTimeout;
    static int connectionTimeout;

    static {
        zkUrl = providerProp.getProperty("provider.zookeeper.address");
        sessionTimeout = Integer.parseInt(providerProp.getProperty("provider.zookeeper.sessionTimeout"));
        connectionTimeout = Integer.parseInt(providerProp.getProperty("provider.zookeeper.connectionTimeout"));
    }


    /**
     * 创建kafka队列
     * @param topicName 队列名称
     * @param partitions 分区数量
     * @param replicationFactor topic中数据的副本数量，默认为3
     *
     */
    public static void createTopic(String topicName, int partitions, int replicationFactor){

        ZkUtils zkUtils = ZkUtils.apply(zkUrl, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
        // 创建一个单分区单副本名为t1的topic
        AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();

    }

    /**
     * 查询所有topic，包括已经被标记删除，还没有删除的topic。
     * @return topic的list
     */
    public static List<String> queryAllTopic(){
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
        ArrayList<String> topics = new ArrayList<String>();
//        AdminUtils.topicExists()
        scala.collection.Map<String, Properties> stringPropertiesMap = AdminUtils.fetchAllTopicConfigs(zkUtils);
        Map<String, Properties> javaMap = JavaConversions.mapAsJavaMap(stringPropertiesMap);
        Iterator<String> iterator = javaMap.keySet().iterator();
        while(iterator.hasNext()){
            String key = iterator.next();
            Properties properties = javaMap.get(key);
            topics.add(key);
        }
        zkUtils.close();
        return  topics;
    }


    /**
     * 删除一个topic，这个删除只是告知系统标记该topic要被删除。而不是立即删除。
     * @param topicName topic的名字
     */
    public static void deleteTopic(String topicName) {
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
        // 删除topic 't1'
        AdminUtils.deleteTopic(zkUtils, topicName);
        zkUtils.close();
    }

    /**
     * 判断一个topic是否已经存在。 包括已经被标记删除，还没有删除的topic。
     * @param topicName topic的名字
     * @return
     */
    public static boolean topicIsExists(String topicName){
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
        boolean topicExists = AdminUtils.topicExists(zkUtils, topicName);
        zkUtils.close();
        return topicExists;
    }


    public static void main(String[] args) {

        String topicName = "testTopic2";
        int partitions = 1;
        int replicationFactor = 1;
//        createTopic(topicName,partitions,replicationFactor);

//        deleteTopic(topicName);
        boolean b = topicIsExists(topicName);
        System.out.println(b);

        List<String> topicList = queryAllTopic();
        for(String s :topicList){
            System.out.println("s->"+  s);
        }

    }

}
