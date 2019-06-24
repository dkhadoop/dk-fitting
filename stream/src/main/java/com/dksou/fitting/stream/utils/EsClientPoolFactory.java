package com.dksou.fitting.stream.utils;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.util.Properties;

public class EsClientPoolFactory implements PooledObjectFactory<TransportClient> {

    static Properties providerProp = PropUtils.getProp("consumer-es.properties");

    String hostips;
    int port;
    String clusterName;
    public EsClientPoolFactory(String hostIps, int port, String clusterName) {
        this.hostips=hostIps;
        this.port=port;
        this.clusterName=clusterName;
    }



    //生成pooledObject
    public PooledObject<TransportClient> makeObject() throws Exception {
        TransportClient client = null;
        /*Settings settings = Settings.builder().put("cluster.name",clusterName).build();

        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostips),port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }*/
        if (!providerProp.getProperty("consumer.es.searchguard.ssl.transport.keystore_filepath").equals( "" )) {
            Settings settings = Settings.builder()
                    .put( "cluster.name", clusterName )
                    .put( "client.transport.sniff", providerProp.getProperty( "consumer.es.client.transport.sniff" ) )//自动探测集群
                    .put( "searchguard.ssl.transport.enabled", providerProp.getProperty( "consumer.es.searchguard.ssl.transport.enabled" ) )
                    .put( "searchguard.ssl.transport.keystore_filepath", providerProp.getProperty( "consumer.es.searchguard.ssl.transport.keystore_filepath" ) )
                    .put( "searchguard.ssl.transport.truststore_password", providerProp.getProperty( "consumer.es.searchguard.ssl.transport.truststore_password" ) )
                    .put( "searchguard.ssl.transport.truststore_filepath", providerProp.getProperty( "consumer.es.searchguard.ssl.transport.truststore_filepath" ) )
                    .put( "searchguard.ssl.transport.keystore_password", providerProp.getProperty( "consumer.es.searchguard.ssl.transport.keystore_password" ) )
                    .put( "searchguard.ssl.transport.enforce_hostname_verification", providerProp.getProperty( "consumer.es.searchguard.ssl.transport.enforce_hostname_verification" ) )
                    .put( "client.transport.ignore_cluster_name", providerProp.getProperty( "consumer.es.client.transport.ignore_cluster_name" ) )
                    .build();
            if (hostips.contains( "," )) {
                TransportClient TransportClient = new PreBuiltTransportClient( settings, SearchGuardSSLPlugin.class );
                //TransportClient TransportClient = new PreBuiltTransportClient( settings);
                String[] hostName = hostips.split( "," );
                for (int i = 0; i < hostName.length; i++) {
                    TransportClient.addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostName[i], port ) ) );
                }
                client = TransportClient;
            } else {
                client = new PreBuiltTransportClient( settings, SearchGuardSSLPlugin.class )
                        .addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostips, port ) ) );
            }
        }else {
            Settings settings = Settings.builder()
                    .put( "cluster.name", clusterName )
                    .put( "client.transport.sniff", providerProp.getProperty( "consumer.es.client.transport.sniff" ))
                    .build();
            if (hostips.contains( "," )) {
                TransportClient TransportClient = new PreBuiltTransportClient( settings );
                String[] hostName = hostips.split( "," );
                for (int i = 0; i < hostName.length; i++) {
                    TransportClient.addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostName[i], port ) ) );
                }
                client = TransportClient;
            } else {
                client = new PreBuiltTransportClient( settings)
                        .addTransportAddresses( new InetSocketTransportAddress( new InetSocketAddress( hostips, port ) ) );
            }
        }

        return new DefaultPooledObject<TransportClient>(client);
    }


    //销毁对象
    public void destroyObject(PooledObject<TransportClient> pooledObject) throws Exception {
        TransportClient client = pooledObject.getObject();
        client.close();
    }


    //验证对象
    public boolean validateObject(PooledObject<TransportClient> pooledObject) {
        return true;
    }


    //激活对象
    public void activateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        // System.out.println("activate esClient");
    }


    //钝化对象
    public void passivateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        // System.out.println("passivate Object");
    }
}