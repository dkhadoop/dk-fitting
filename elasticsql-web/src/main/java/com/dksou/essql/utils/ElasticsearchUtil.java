package com.dksou.essql.utils;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchUtil {
    public Client getEsClient(Map<String, Integer> esAddresses,Map<String, String> esConfig){
        Settings settings = Settings.builder().put(esConfig).put("client.transport.sniff", true).build();
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        for (Map.Entry<String, Integer> esIpAndPort: esAddresses.entrySet()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(esIpAndPort.getKey(), esIpAndPort.getValue())));
        }
        List<DiscoveryNode> discoveryNodes = transportClient.connectedNodes();
        if (discoveryNodes.isEmpty()) {
            throw new RuntimeException("Cannot connect to any elasticsearch nodes");
        }
       return transportClient;
    }

}
