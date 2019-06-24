package com.dksou.fitting.search.SearchService.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.dksou.fitting.search.SearchService.SearchService;
import com.dksou.fitting.utils.ESUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

/**
 * @create: 08-14-15
 * @author:
 **/
public class SearchServiceImpl implements SearchService.Iface {
    private static Logger logger = Logger.getLogger(SearchServiceImpl.class);
    /**
     * 条件查询，达到1500提前结束，设置是否按查询匹配度排序，精确查询
     * @param hostIps ES集群的ip地址
     * @param clusterName ES集群集群名称
     * @param indexName ES集群的索引名称，可使用多个索引 indexName="test2,test1,test";
     * @param typeName 索引类型，可多个 typeName="doc,pdf,test";
     * @param port ES集群的端口号
     * @param start 记录偏移 , null-默认为0
     * @param size 记录偏移 , null-默认为10
     * @param sentence 需要搜索的词语，
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> ConditionalQuery(String hostIps, String clusterName, String indexName, String typeName, int port, int start, int size, String sentence) throws TException {

        Client client=null;
        try {
             client = ESUtils.getClient( hostIps, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        String[] index;
        if (indexName.contains( "," )){
            index = indexName.split( "," );
        }else {
            index = new String[]{indexName};
        }
        String[] type;
        if (typeName.contains( "," )){
            type = typeName.split( "," );
        }else {
            type = new String[]{typeName};
        }
        int start2 = start;
        int size2 = (Integer)size == null ? 10 : size;
        SearchResponse response = client.prepareSearch(index)//可以是多个index
                .setTypes(type)//可以是多个类型
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                //.setQuery(QueryBuilders.matchAllQuery())    // Query 查询全部
                .setQuery(  QueryBuilders.queryStringQuery( sentence ) )
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter 过滤
                .setFrom(start2).setSize(size2).setExplain(true)////设置是否按查询匹配度排序
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)//精确查询
                .setTerminateAfter(1000)//达到1000提前结束
                .get();
        long totalHits = 0;
        SearchHit[] searchHits = response.getHits().getHits();
        String resp=null;
        Map map=new LinkedHashMap(  );
        for (SearchHit searchHit : searchHits) {
            resp = JSON.toJSONString( searchHit.getSource(), SerializerFeature.PrettyFormat );
            totalHits++;
            //System.out.println( "resp"+resp );
            map.put( String.valueOf(totalHits ),resp );
        }
        long totalHits1 = response.getHits().totalHits;
        map.put( "count",String.valueOf(totalHits1 ) );

        return map;
    }

    /**
     * 模糊查询，通配符查询,一个问号代表一个字符，*号代表通配
     * @param hostIps ES集群的ip地址
     * @param clusterName ES集群集群名称
     * @param indexName ES集群的索引名称，可使用多个索引 indexName="test2,test1,test";
     * @param typeName 索引类型，可多个 typeName="doc,pdf,test";
     * @param port ES集群的端口号
     * @param start 记录偏移 , null-默认为0
     * @param size 记录偏移 , null-默认为10
     * @param sentence 需要搜索的词语，
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> FuzzyQuery(String hostIps, String clusterName, String indexName, String typeName, int port, int start, int size, String sentence, String field) throws TException {

        Client client=null;
        try {
            client = ESUtils.getClient( hostIps, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        String[] index;
        if (indexName.contains( "," )){
            index = indexName.split( "," );
        }else {
            index = new String[]{indexName};
        }
        String[] type;
        if (typeName.contains( "," )){
            type = typeName.split( "," );
        }else {
            type = new String[]{typeName};
        }
        int start2 = (Integer)start == null ? 0 : start;
        int size2 = (Integer)size == null ? 10 : size;
        Map map=new LinkedHashMap(  );
        long totalHits = 0;
        QueryBuilder qb=null;
        if ((sentence.contains( "?" )||sentence.contains( "*" )||sentence.contains( "？" ))&&(field!=null&&!field.equals( " " ))) {
            if(sentence.contains( "？" )){
                StringBuilder sb=new StringBuilder(  );
                sb.append( sentence.replaceAll( "？","?" ) );
                sentence=sb.toString();
            }
             qb = wildcardQuery( field, sentence );
        }else if (field!=null&&!field.equals( " " )){
            qb = fuzzyQuery( field, sentence).fuzziness(Fuzziness.TWO);
        }
        SearchResponse response = client.prepareSearch(index)//可以是多个index
                .setTypes(type)//可以是多个类型
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)//精确查询
                //.setQuery(QueryBuilders.matchAllQuery())    // Query 查询全部
                .setQuery(  qb )
                .setFrom(start2).setSize(size2).setExplain(true)////设置是否按查询匹配度排序
                .setTerminateAfter(1000)//达到1000提前结束
                .get();
        SearchHit[] searchHits = response.getHits().getHits();
        String resp=null;

        for (SearchHit searchHit : searchHits) {
            resp = JSON.toJSONString( searchHit.getSource(), SerializerFeature.PrettyFormat );
            totalHits++;
            map.put( String.valueOf(totalHits ),resp );
        }
        long totalHits1 = response.getHits().totalHits;
        map.put( "count",String.valueOf(totalHits1 ) );

        return map;
    }



    /**
     * 过滤查询，
     * @param hostIps ES集群的ip地址
     * @param clusterName ES集群集群名称
     * @param indexName ES集群的索引名称，可使用多个索引 indexName="test2,test1,test";
     * @param typeName 索引类型，可多个 typeName="doc,pdf,test";
     * @param port ES集群的端口号
     * @param start 记录偏移 , null-默认为0
     * @param size 记录偏移 , null-默认为10
     * @param field  字段名， 例如："age,balance"
     * @param gte  过滤条件(大于等于) 例如："20,1000"
     * @param lte  过滤条件(小于等于) 例如："23,2000"
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> FileterQuery(String hostIps, String clusterName, String indexName, String typeName, int port, int start, int size, String field, String gte, String lte) throws TException {
        Client client=null;
        try {
            client = ESUtils.getClient( hostIps, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        int start2 = (Integer)start == null ? 0 : start;
        int size2 = (Integer)size == null ? 10 : size;
        //声明并添加过滤条件；
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //boolQueryBuilder.must(QueryBuilders.matchAllQuery());

       if (field.contains( "," )||gte.contains( "," )||lte.contains( "," )){
           String[] fields=field.split( "," );
           String[] gtes=gte.split( "," );
           String[] ltes=lte.split( "," );

           for (int i = 0; i <fields.length ; i++) {
               boolQueryBuilder.filter( QueryBuilders.rangeQuery( fields[i] ).gte( gtes[i] ).lte( ltes[i] ) );
               System.out.println(  "field = [" + fields[i] + "], gte = [" + gtes[i] + "], lte = [" + ltes[i] + "]" );
           }
       }else {
           boolQueryBuilder.filter( QueryBuilders.rangeQuery( field ).gte( gte ).lte( lte ) );
       }
        String[] index;
        if (indexName.contains( "," )){
            index = indexName.split( "," );
        }else {
            index = new String[]{indexName};
        }
        String[] type;
        if (typeName.contains( "," )){
            type = typeName.split( "," );
        }else {
            type = new String[]{typeName};
        }
        SearchResponse response = client.prepareSearch(index)//可以是多个index
                .setTypes(type)//可以是多个类型
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery( boolQueryBuilder )
                .setFrom(start2).setSize(size2).setExplain(true)
                .get();
        SearchHit[] hits1 = response.getHits().getHits();
        Map<String,String> map=new LinkedHashMap<>(  );
        long totalHits =0;
        for (SearchHit searchHit:
             hits1) {
            String s = JSON.toJSONString( searchHit.getSource(), SerializerFeature.PrettyFormat );
            totalHits++;
            map.put(  String.valueOf(totalHits ),s );
        }
        long totalHits1 = response.getHits().totalHits;
        map.put( "count",String.valueOf( totalHits1 ) );

        return map;
    }
    /**
     * 聚合统计，
     * @param hostIp ES集群的ip地址
     * @param clusterName ES集群集群名称
     * @param indexName ES集群的索引名称，可使用多个索引 indexName="test2,test1,test";
     * @param typeName 索引类型，可多个 typeName="doc,pdf,test";
     * @param port ES集群的端口号
     * @param aggFdName 需要统计的字段
     * @param aggType 记录偏移 , null-默认为10
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> StatsAggregation(String hostIp, int port, String clusterName, String indexName, String typeName, String aggFdName, String aggType) throws TException {
        Client client=null;
        try {
            client = ESUtils.getClient( hostIp, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        ExtendedStatsAggregationBuilder aggregationBuilder = AggregationBuilders.extendedStats( "agg" ).field( aggFdName );
        SearchResponse response = client.prepareSearch( indexName ).addAggregation( aggregationBuilder ).get();
        ExtendedStats agg = response.getAggregations().get( "agg" );
        Map<String,String> map=new LinkedHashMap<>(  );
        map.put("avg", agg.getAvgAsString() );
        map.put("count", String.valueOf( agg.getCount() ) );
        map.put("sum", agg.getSumAsString() );
        map.put("max", agg.getMaxAsString() );
        map.put("min", agg.getMinAsString() );
        //以字符串形式收集的值的标准偏差。
        map.put( "StdDeviation",agg.getStdDeviationAsString() );
        //平方和
        map.put( "SumOfSquares" ,agg.getSumOfSquaresAsString());
        //方差
        map.put( "Variance",agg.getStdDeviationAsString() );
        System.out.println( "stats avg"+agg.getAvgAsString()+"count"+agg.getCount()+"max"+agg.getMaxAsString()+"min"+agg.getMinAsString()+"sum"+agg.getSumAsString() );

        return map;
    }


    /**
     *
     *对指定字段（脚本）的值按从小到大累计每个值对应的文档数的占比（占所有命中文档数的百分比），返回指定占比比例对应的值。
     * 默认返回[ 1, 5, 25, 50, 75, 95, 99 ]分位上的值。如下中间的结果，可以理解为：占比为50%的文档的age值 <= 31，
     * 或反过来：age<=31的文档数占总命中文档数的50%
     *  也可以自己指定分位值，如 10,50,90.
     * @param hostIp         ES集群的ip地址
     * @param port ES集群的端口号
     * @param clusterName  ES集群名称
     * @param indexName  ES集群的索引名称，可使用多个索引 indexName="test2,test1,test";
     * @param typeName 索引类型，可多个 typeName="doc,pdf,test";
     * @param aggFdName 字段名称
     * @param percentiles 自定义百分比分位数，比如10,50,90 之间以","分隔
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> PercentilesAggregation(String hostIp, int port, String clusterName, String indexName, String typeName, String aggFdName, String percentiles) throws TException {
        Client client=null;
        try {
            client = ESUtils.getClient( hostIp, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        PercentilesAggregationBuilder aggregation=null;
        if(percentiles==null&&percentiles.equals( "" )) {
            aggregation =
                    AggregationBuilders.percentiles( aggFdName + "_percentiles" ).field( aggFdName );
        }else {
            double[] parseDouble=new double[10];
            if(percentiles.contains( "," )){
                String[] split = percentiles.split( "," );
                for (int i = 0; i <split.length ; i++) {
                    parseDouble[i] = Double.parseDouble( split[i] );
                }
            }else {
                parseDouble=new double[]{Double.parseDouble( percentiles )};
            }
            aggregation =
                    AggregationBuilders.percentiles( aggFdName + "_percentiles" ).field( aggFdName ).percentiles(  parseDouble  );
        }
        SearchResponse sr = client.prepareSearch(indexName)
                                    .addAggregation(aggregation)
                                    .get();
        Percentiles percentiles1 = sr.getAggregations().get( aggFdName+"_percentiles" );
        Map map=new LinkedHashMap(  );
        for (Percentile percentile:percentiles1) {
            System.out.println("global " + percentile.getPercent() + " count:" + percentile.getValue());
            map.put( String.valueOf(percentile.getPercent()+"%" ),String.valueOf(aggFdName+"="+ percentile.getValue()) );
        }
        return map;
    }
    /**
     *
     * Terms Aggregation  根据字段值项分组聚合
     * @param hostIp ES集群的IP
     * @param port ES集群的端口号
     * @param clusterName ES集群的名称
     * @param indexName ES的索引名称
     * @param typeName ES的类型名称
     * @param aggFdName 需要聚合的字段
     * @param
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> TermsAggregation(String hostIp, int port, String clusterName, String indexName, String typeName, String aggFdName) throws TException {
        Client client=null;
        try {
            client = ESUtils.getClient( hostIp, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        String[] index;
        if (indexName.contains( "," )){
            index = indexName.split( "," );
        }else {
            index = new String[]{indexName};
        }
        String[] type;
        if (typeName.contains( "," )){
            type = typeName.split( "," );
        }else {
            type = new String[]{typeName};
        }
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .addAggregation(
                        AggregationBuilders.terms(aggFdName+"_terms")
                                           .field(aggFdName))
                .addSort( aggFdName,SortOrder.DESC )
                .get();
        Terms by_gender = response.getAggregations().get( aggFdName+"_terms" );
        Map<String,String> map=new LinkedHashMap<>(  );
        for (Terms.Bucket term:
                by_gender.getBuckets()) {
            System.out.println(term.getKey()+"==="+term.getDocCount());
            String keyAsString = term.getKeyAsString();
            String s = String.valueOf( term.getDocCount() );
            map.put(keyAsString,s);
        }
        if (map.isEmpty()){
            map.put( "0","0" );
        }
        return map;
    }

    /**
     *根据多条件组合与查询 （相当于sql里的and,age=32 and gender=m,也可以单个条件查询age=32. ）
     * @param hostIp ES集群的IP地址
     * @param port 连接ES集群的端口号
     * @param clusterName ES集群的名称
     * @param indexName ES集群的索引
     * @param typeName ES集群的类型名称
     * @param filed 查询的字段名称  比如：age，gender
     * @param filedName 查询的内容  比如：32，m
     * @param start 记录偏移 , null-默认为0
     * @param size 记录偏移 , null-默认为10
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> MustSearch(String hostIp, int port, String clusterName, String indexName, String typeName, String filed, String filedName, int start, int size) throws TException {
        Client client=null;
        try {
            client = ESUtils.getClient( hostIp, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        int start2 = (Integer)start == null ? 0 : start;
        int size2 = (Integer)size == null ? 10 : size;
        //String field="age,gender";
        //String Type="32,m";
        if (filed.contains( "," )||filedName.contains( "," )){
            String[] split = filed.split( "," );
            String[] split1= filedName.split( "," );
            for (int i = 0; i <split.length ; i++) {
                boolQueryBuilder.must( QueryBuilders.termsQuery( split[i], split1[i]) );
            }
        }else {
             boolQueryBuilder.must( QueryBuilders.termsQuery( filed, filedName ) );
        }
        String[] index;
        if (indexName.contains( "," )){
            index = indexName.split( "," );
        }else {
            index = new String[]{indexName};
        }
        String[] type;
        if (typeName.contains( "," )){
            type = typeName.split( "," );
        }else {
            type = new String[]{typeName};
        }
        SearchResponse response = client.prepareSearch( index ).setTypes( type )
                .setQuery( boolQueryBuilder ).setFrom( start2 ).setSize( size2 ).get();
        SearchHit[] hits1 = response.getHits().getHits();
        Map<String,String> map=new LinkedHashMap<>(  );
        long totalHits =0;
        for (SearchHit searchHit:
                hits1) {
            String s = JSON.toJSONString( searchHit.getSource(), SerializerFeature.PrettyFormat );
            totalHits++;
            map.put(  String.valueOf(totalHits ),s );
        }
        long totalHits1 = response.getHits().totalHits;
        map.put( "count",String.valueOf( totalHits1 ) );

        return map;
    }

    /**
     *多条件或查询（相当于sql里的or，age=32 or gender=m,也可以单个条件查询age=32.）
     * @param hostIp ES集群的IP
     * @param port ES集群的端口号
     * @param clusterName ES集群名称
     * @param indexName 索引
     * @param typeName 类型
     * @param filed 字段  比如：age，gender
     * @param filedName 查询内容  比如：32，m
     * @param start 记录偏移
     * @param size 记录偏移
     * @return
     * @throws TException
     */
    @Override
    public Map<String, String> ShouldSearch(String hostIp, int port, String clusterName, String indexName, String typeName, String filed, String filedName, int start, int size) throws TException {
        Client client=null;
        try {
            client = ESUtils.getClient( hostIp, port, clusterName );
        } catch (Exception e) {
            e.printStackTrace();
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //String field="age,gender";
        //String Type="32,m";
        int start2 = (Integer)start == null ? 0 : start;
        int size2 = (Integer)size == null ? 10 : size;
        if (filed.contains( "," )||filedName.contains( "," )){
            String[] split = filed.split( "," );
            String[] split1= filedName.split( "," );
            for (int i = 0; i <split.length ; i++) {
                boolQueryBuilder.should( QueryBuilders.termsQuery( split[i], split1[i]) );
            }
        }else {
            boolQueryBuilder.should( QueryBuilders.termsQuery( filed, filedName ) );
        }
        String[] index;
        if (indexName.contains( "," )){
            index = indexName.split( "," );
        }else {
            index = new String[]{indexName};
        }
        String[] type;
        if (typeName.contains( "," )){
            type = typeName.split( "," );
        }else {
            type = new String[]{typeName};
        }
        SearchResponse response = client.prepareSearch( index ).setTypes( type )
                .setQuery( boolQueryBuilder ).setFrom( start2 ).setSize( size2 ).get();
        SearchHit[] hits1 = response.getHits().getHits();
        Map<String,String> map=new LinkedHashMap<>(  );
        long totalHits =0;
        for (SearchHit searchHit:
                hits1) {
            String s = JSON.toJSONString( searchHit.getSource(), SerializerFeature.PrettyFormat );
            totalHits++;
            map.put(  String.valueOf(totalHits ),s );
        }
        long totalHits1 = response.getHits().totalHits;
        map.put( "count",String.valueOf( totalHits1 ) );

        return map;
    }

}
