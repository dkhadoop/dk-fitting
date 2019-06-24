package com.dksou.fitting.search.SearchIOService.impl;

import com.dksou.fitting.search.SearchIOService.DKSearchOutput;
import com.dksou.fitting.utils.ESUtils;
import com.dksou.fitting.utils.ExportExcelUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;


import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * DKSearchInput的实现类
 *
 */
public class DKSearchOutputImpl implements DKSearchOutput.Iface {

    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public  static Client client;

    /**
     *
     * 获取ES某个索引下数据的总和
     * @param hostIps
     * @param clusterName
     * @param indexName
     * @param typeName
     * @param port
     * @return
     */
    @Override
    public long getESSum(String hostIps, String clusterName, String indexName, String typeName, int port) throws Exception {
        client=ESUtils.getClient( hostIps,port,clusterName );
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists( indexName ).execute().actionGet();
        if (!existsResponse.isExists()){
            System.out.println( "index Non-existent " );
            return -1;
        }
        TypesExistsResponse typesExistsResponse = client.admin().indices().prepareTypesExists( indexName ).setTypes( typeName ).execute().actionGet();
        if (!typesExistsResponse.isExists()){
            System.out.println( "type Non-existent " );
            return -1;
        }
        return client.prepareSearch( indexName ).setTypes( typeName ).get().getHits().totalHits;
    }

    /**
     *
     * 导出es的数据为txt文本
     * @param hostIps
     * @param clusterName
     * @param indexName
     * @param typeName
     * @param port
     * @param start
     * @param size
     * @return
     * @throws TException
     */
    @Override
    public String ES2Txt(String hostIps, String clusterName, String indexName, String typeName, int port, int start, int size) throws Exception {
        String type=".txt";
        //把导出的数据以JSON格式写到文件里
        client=ESUtils.getClient( hostIps,port,clusterName );
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists( indexName ).execute().actionGet();
        if (!existsResponse.isExists()){
            return "index Non-existent ";
        }
        TypesExistsResponse typesExistsResponse = client.admin().indices().prepareTypesExists( indexName ).setTypes( typeName ).execute().actionGet();
        if (!typesExistsResponse.isExists()){
            return "type Non-existent";
        }
        BufferedWriter out=new BufferedWriter( new FileWriter( indexName+"_"+typeName+"_"+System.currentTimeMillis()+type,true ) );
        SearchResponse response=null;
        SearchRequestBuilder requestBuilder=client.prepareSearch( indexName ).setTypes( typeName ).setQuery( QueryBuilders.matchAllQuery() );
        response=requestBuilder.setFrom( start ).setSize( size).execute().actionGet();
        SearchHits searchHits=response.getHits();
        if (searchHits==null){return "no searchHit";}
        for (int i = 0; i <searchHits.getHits().length ; i++) {
            SearchHit searchHitFields = searchHits.getHits()[i];
            Collection<Object> values1 = searchHitFields.getSource().values();
            Object[] values=null;
            try {
                values = values1.toArray( new Object[values1.size()] );
            }catch (ArrayStoreException e){
                Iterator<Object> iterator = values1.iterator();
                HashMap<String,Object> next=(HashMap<String, Object>)iterator.next();
                values=next.values().toArray();
            }
            StringBuilder sb=new StringBuilder(  );
            for (Object object: values) {
                sb.append( object+"|" );

            }
            sb.deleteCharAt( sb.length()-1 );

            out.write( sb.toString() );
            out.write( "\r\n" );
        }
    out.close();
        return "SUCCESS";
    }

    /**
     * 从es导出数据，导出为xls
     * @param hostIps
     * @param clusterName
     * @param indexName
     * @param typeName
     * @param port
     * @param start
     * @param size
     * @return
     * @throws TException
     */
    @Override
    public String ES2XLS(String hostIps, String clusterName, String indexName, String typeName, int port, int start, int size) throws Exception {
        String type=".xlsx";
        client=ESUtils.getClient( hostIps,port,clusterName );
        IndicesExistsResponse existsResponse = client.admin().indices().prepareExists( indexName ).execute().actionGet();
        if (!existsResponse.isExists()){
            return "index Non-existent ";
        }
        TypesExistsResponse typesExistsResponse = client.admin().indices().prepareTypesExists( indexName ).setTypes( typeName ).execute().actionGet();
        if (!typesExistsResponse.isExists()){
            return "type Non-existent";
        }
        //把导出的数据以json格式写到文件里，
        BufferedOutputStream out=new BufferedOutputStream( new FileOutputStream( indexName+"_"+typeName+"_"+System.currentTimeMillis()+type,true) );
        SearchResponse response=null;

        long totalHits = client.prepareSearch( indexName ).setTypes( typeName ).get().getHits().totalHits;
        //setSearchType(SearchType.Scan) 告诉ES不需要排序只要结果返回即可 setScroll(new TimeValue(600000)) 设置滚动的时间
        //每次返回数据10000条。一直循环查询直到所有的数据都查询出来
        //setTerminateAfter(10000)    //如果达到这个数量，提前终止
        SearchRequestBuilder requestBuilder = client.prepareSearch( indexName ).setTypes( typeName ).setQuery( QueryBuilders.matchAllQuery() ).setScroll( new TimeValue(600000) );
        response=requestBuilder.setFrom( start ).setSize( size ).execute().actionGet();
        String title="ES导出";
        title=URLEncoder.encode( title,"UTF-8" );
        SearchHits searchHits = response.getHits();
        if (searchHits==null){return "no searchHit";}
        String[] keys=null;
        ArrayList<Object[]> list=new ArrayList<>(  );
        for (int i = 0; i <searchHits.getHits().length ; i++) {
            Set<String> keySet = searchHits.getHits()[i].getSource().keySet();
            keys = keySet.toArray( new String[keySet.size()] );
            Collection<Object> values = searchHits.getHits()[i].getSource().values();
            Object[] object = values.toArray( new Object[values.size()] );
            list.add( object );
        }
         ExportExcelUtils.exportExcelXSSF( title, keys, list, out );

        out.close();
        return "SUCCESS";
    }




}
