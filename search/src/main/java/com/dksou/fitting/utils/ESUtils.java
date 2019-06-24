package com.dksou.fitting.utils;


import com.dksou.fitting.utils.EsClientPoolFactory;
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

import java.util.Map;

public class ESUtils {
    static Map<String, String> prop = com.dksou.fitting.utils.PropUtils.getProp("dkSearch.properties");

    public  static Client client;

    public static Client getClient(String hostIps, int port, String clusterName){
        //System.out.println( "hostIps = [" + hostIps + "], port = [" + port + "], clusterName = [" + clusterName + "]" );
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();//对象池配置类，不写也可以，采用默认配置
        //poolConfig.setMaxTotal(list.length);//采用默认配置maxTotal是8，池中有8个client
        // System.out.println(Integer.parseInt( prop.get("dkSearch.commons-pool.MaxTotal") )  );
        poolConfig.setMaxTotal( Integer.parseInt( prop.get("dkSearch.commons-pool.MaxTotal") ) );

        poolConfig.setMaxIdle(Integer.parseInt(prop.get("dkSearch.commons-pool.MaxIdlel")));
        poolConfig.setMinIdle(Integer.parseInt(prop.get("dkSearch.commons-pool.MinIdle")));
        poolConfig.setMaxWaitMillis(Integer.parseInt(prop.get("dkSearch.commons-pool.MaxWaitMillis")));
        EsClientPoolFactory esClientPoolFactory = new EsClientPoolFactory(hostIps,port,clusterName);//要池化的对象的工厂类，这个是我们要实现的类
        GenericObjectPool<TransportClient> clientPool = new GenericObjectPool<>(esClientPoolFactory,poolConfig);//利用对象工厂类和配置类生成对象池
        try {
            client = clientPool.borrowObject(); //从池中取一个对象
        } catch (Exception e) {
            e.printStackTrace();
        }
        clientPool.returnObject( (TransportClient) client );//使用完毕之后，归还对象
        return client;
    }
    /*// 5.6.8的连接
    public static Client getClient(String hostIps, int port, String clusterName) {
        System.out.println( "hostIps = [" + hostIps + "], port = [" + port + "], clusterName = [" + clusterName + "]" );
        try {
        if (client == null) {
            if (!prop.get("searchguard.ssl.transport.keystore_filepath").equals( "" )) {
                Settings settings = Settings.builder()
                        .put( "cluster.name", clusterName )
                        .put( "client.transport.sniff", prop.get( "client.transport.sniff" ) )//自动探测集群
                        .put( "searchguard.ssl.transport.enabled", prop.get( "searchguard.ssl.transport.enabled" ) )
                        .put( "searchguard.ssl.transport.keystore_filepath", prop.get( "searchguard.ssl.transport.keystore_filepath" ) )
                        .put( "searchguard.ssl.transport.truststore_password", prop.get( "searchguard.ssl.transport.truststore_password" ) )
                        .put( "searchguard.ssl.transport.truststore_filepath", prop.get( "searchguard.ssl.transport.truststore_filepath" ) )
                        .put( "searchguard.ssl.transport.keystore_password", prop.get( "searchguard.ssl.transport.keystore_password" ) )
                        .put( "searchguard.ssl.transport.enforce_hostname_verification", prop.get( "searchguard.ssl.transport.enforce_hostname_verification" ) )
                        .put( "client.transport.ignore_cluster_name", prop.get( "client.transport.ignore_cluster_name" ) )
                        .build();
                if (hostIps.contains( "," )) {
                    TransportClient TransportClient = new PreBuiltTransportClient( settings, SearchGuardSSLPlugin.class );
                    //TransportClient TransportClient = new PreBuiltTransportClient( settings);
                    String[] hostName = hostIps.split( "," );
                    for (int i = 0; i < hostName.length; i++) {
                        TransportClient.addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostName[i], port ) ) );
                    }
                    client = TransportClient;
                } else {
                    client = new PreBuiltTransportClient( settings, SearchGuardSSLPlugin.class )
                            .addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostIps, port ) ) );
                }

//            ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
//            int numberOfDataNodes = healths.getNumberOfDataNodes();
//            DKCheckUtils.check(numberOfDataNodes);
            }else {
                Settings settings = Settings.builder()
                        .put( "cluster.name", clusterName )
                        .put( "client.transport.sniff", prop.get( "client.transport.sniff" ))
                        .build();
                if (hostIps.contains( "," )) {
                    TransportClient TransportClient = new PreBuiltTransportClient( settings );
                    String[] hostName = hostIps.split( "," );
                    for (int i = 0; i < hostName.length; i++) {
                        TransportClient.addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostName[i], port ) ) );
                    }
                    client = TransportClient;
                } else {
                    client = new PreBuiltTransportClient( settings)
                            .addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostIps, port ) ) );
                }
            }
        }
        } catch (Exception e) {
            e.printStackTrace();
            if (client != null) {
                client.close();
            }
            return null;
        }
        //client.close();


       // client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();
        return client;
    }
    public static void close(){
        if (client != null) {
            client.close();
        }
    }*/
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

    //创建index客户端
    public static void createIndex(String hostIps,int port,String clusterName,String indexName,String typeName) {
        try {
            client = getClient(hostIps,port,clusterName);
            String index = indexName; // 索引名
            String type = typeName; // 类型，类似表名
            IndicesExistsResponse indicesExistsResponse = client.admin()
                    .indices().prepareExists(index).execute().actionGet();
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
}
