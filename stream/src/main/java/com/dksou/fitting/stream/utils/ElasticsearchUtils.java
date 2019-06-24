package com.dksou.fitting.stream.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.util.Properties;


public class ElasticsearchUtils {

    static Properties providerProp = PropUtils.getProp("consumer-es.properties");


    public  static Client client;

    public static Client getClient(String hostIps, int port, String clusterName){
        //System.out.println( "hostIps = [" + hostIps + "], port = [" + port + "], clusterName = [" + clusterName + "]" );
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();//对象池配置类，不写也可以，采用默认配置
        //poolConfig.setMaxTotal(list.length);//采用默认配置maxTotal是8，池中有8个client
        // System.out.println(Integer.parseInt( prop.get("dkSearch.commons-pool.MaxTotal") )  );
        poolConfig.setMaxTotal( Integer.parseInt( providerProp.getProperty("consumer.es.dkSearch.commons-pool.MaxTotal") ) );

        poolConfig.setMaxIdle(Integer.parseInt(providerProp.getProperty("consumer.es.dkSearch.commons-pool.MaxIdlel")));
        poolConfig.setMinIdle(Integer.parseInt(providerProp.getProperty("consumer.es.dkSearch.commons-pool.MinIdle")));
        poolConfig.setMaxWaitMillis(Integer.parseInt(providerProp.getProperty("consumer.es.dkSearch.commons-pool.MaxWaitMillis")));
        EsClientPoolFactory esClientPoolFactory = new EsClientPoolFactory(hostIps,port,clusterName);//要池化的对象的工厂类，这个是我们要实现的类
        GenericObjectPool<TransportClient> clientPool = new GenericObjectPool<TransportClient>(esClientPoolFactory, poolConfig);//利用对象工厂类和配置类生成对象池
        try {
            client = clientPool.borrowObject(); //从池中取一个对象
        } catch (Exception e) {
            e.printStackTrace();
        }
        clientPool.returnObject( (TransportClient) client );//使用完毕之后，归还对象
        return client;
    }

    public static IndexResponse insert(String hostIps, int port, String clusterName, String index, String type, String id, XContentBuilder b) throws Exception{
        client = getClient(hostIps,port,clusterName);
        //插入数据
        IndexRequestBuilder indexBuilder = client.prepareIndex(index, type, id).setSource(b);
        IndexResponse response = indexBuilder.execute().actionGet();
        return response;
    }

    public static IndexResponse insert2(String hostIps, int port,String clusterName ,String index, String type, XContentBuilder b) throws Exception{
        client = getClient(hostIps,port,clusterName);
        //插入数据
        IndexRequestBuilder indexBuilder = client.prepareIndex(index, type).setSource(b);
        IndexResponse response = indexBuilder.execute().actionGet();
        return response;
    }

    //创建index客户端 使用
    public static void createIndex(String hostIps,int port,String clusterName,String indexName,String typeName) {
        try {
            client = getClient(hostIps,port,clusterName);
            String index = indexName; // 索引名
            String type = typeName; // 类型，类似表名
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index).execute().actionGet();
            if(!indicesExistsResponse.isExists()){
                IndicesAdminClient ac = client.admin().indices();//针对索引的操作/管理
                CreateIndexRequestBuilder builder = ac.prepareCreate(index);
                CreateIndexResponse indexresponse = builder.execute().actionGet();
            }
            else {
                System.out.println("索引："+ indexName +"已存在");
            }
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 发送方法
     * @param esHostIp
     * @param esPort
     * @param esClusterName
     * @param index
     * @param type
     * @param message
     * @param separator
     * @param isJsonMessage
     * @throws Exception
     */
    public static void sendToES(String esHostIp, int esPort,String esClusterName,String index, String type, String message,String separator,boolean isJsonMessage) throws Exception{
        client = getClient(esHostIp, esPort, esClusterName);
        if(!isJsonMessage) {
            JSONObject map=new JSONObject();

            String[] splits = message.split(separator);
            for (int i = 0; i < splits.length; i++) {
                map.put("v" + (i+1), splits[i]);
            }
            String s = map.toJSONString();
//            System.out.println("s = " + s);
            client.prepareIndex(index, type).setSource(s).execute().actionGet();
        }else {
            client.prepareIndex(index, type).setSource(message).execute().actionGet();
        }
    }


}
