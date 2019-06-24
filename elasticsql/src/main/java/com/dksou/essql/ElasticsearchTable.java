package com.dksou.essql;

import com.dksou.essql.utils.ElasticSearchFieldType;
import com.dksou.essql.utils.TwoTuple;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;

/**
 * Created by myy on 2017/6/29.
 */

public class ElasticsearchTable extends AbstractTable implements TranslatableTable {

    Client client;
    String esIndexName;
    String esTypeName;

    protected RelDataType rowType;
    protected RelDataType rowType2;
    protected final List<QueryBuilder> queryBuilderList = new ArrayList<>();
    protected final List<AbstractAggregationBuilder> aggregationBuilderList = new ArrayList<>();
    protected final List<TermsAggregationBuilder> aggregationBuilderList2 = new ArrayList();  //group后order的情况
    protected final List<SortBuilder> sortBuilderList = new ArrayList<>();
    protected int searchOffset;
    protected int searchSize = 10;
    protected Boolean isGroup = false;              //是否是分组
    protected List<String> out = new ArrayList<String>();  //存放分组时聚合的name
    protected List<String> outNames = new ArrayList<String>(); //输出顺序及名称  如:prod_price,$ 代表slect prod_price,count(prod_price)
    protected Boolean isSort = false;  //是否排序  分组排序用
    protected Boolean isAsc;    //是否正序  分组排序用

    public void setIsSort(Boolean sortFlag) {
        isSort = sortFlag;
    }

    public void setIsAsc(Boolean ascFlag) {
        isAsc = ascFlag;
    }

    public void setOutNames(List outNames) {
        this.outNames = outNames;
    }

    public void setOut(List<String> arr) {
        this.out = arr;
    }

    public void setIsGroup(Boolean isGroup) {           //是否为group函数
        this.isGroup = isGroup;
    }

    public void setSearchOffset(int searchOffset) {
        this.searchOffset = searchOffset;
    }

    public void setSearchSize(int searchSize) {
        this.searchSize = searchSize;
    }

    public void addQueryBuilder(QueryBuilder queryBuilder) {
        queryBuilderList.add(queryBuilder);
    }

    public void addAggregationBuilderList2(TermsAggregationBuilder agg) {
        aggregationBuilderList2.add(agg);
    }

    public void addAggregationBuilder(AbstractAggregationBuilder aggregationBuilder, Class cls) {
        aggregationBuilderList.add(aggregationBuilder);
    }

    public void addSortBuilder(String field, SortOrder order) {
        sortBuilderList.add(SortBuilders.fieldSort(field).order(order));
    }

    public ElasticsearchTable(Client esClient, String esIndexName, String esTypeName) {
        this.client = esClient;
        this.esIndexName = esIndexName;
        this.esTypeName = esTypeName;
    }


    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null)
            try {
                rowType = getRowTypeFromEs(typeFactory)[0];
                rowType2 = getRowTypeFromEs(typeFactory)[1];
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        return rowType2;
    }

    public RelDataType getRowType() {
        return rowType2;
    }

    public RelDataType getRowType2() {
        return rowType;
    }


    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        return new ElasticsearchTableScan(cluster, cluster.traitSetOf(ElasticsearchRelNode.CONVENTION),
                relOptTable, this, null);
    }

    public TwoTuple<List<Object>, List<Object[]>> find(List<ElasticSearchFieldType> typeList, List<String> listField, boolean oneColumFlag) {
        List<Object[]> result = new ArrayList<Object[]>();
        List<Object> singleResult = new ArrayList<Object>();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(esIndexName).setTypes(esTypeName);

        if (aggregationBuilderList.size() == 0 && queryBuilderList.size() == 0) {
            if (listField.size() == 0) {            //select * 的情况
                List<RelDataTypeField> fieldList = getRowType2().getFieldList();
                for (RelDataTypeField f : fieldList) {
                    String name = f.getName().toLowerCase();
                    listField.add(name);
                }
                ArrayList<String> lf2 = new ArrayList<>(); //lf2为 去重后的listField ,即id和id.keyword 去掉id
                for (String fn : listField) {
                    if (fn.endsWith(".keyword") || !listField.contains(fn + ".keyword")) {
                        searchRequestBuilder.addDocValueField(fn);
                    }
                    if (!fn.endsWith(".keyword")) {
                        lf2.add(fn);
                    }
                }
                listField = lf2;
            } else {
                for (String field2 : listField) {
//                boolQueryBuilder.must(QueryBuilders.termQuery(field,null));
                    if (field2.contains(",")) {
//                    searchRequestBuilder.addField(field2.split(",")[1]);
                        searchRequestBuilder.addDocValueField(field2.split(",")[1]);
                    } else {
                        List<RelDataTypeField> fieldList = getRowType2().getFieldList();
                        searchRequestBuilder.addDocValueField(transFieldName(field2, fieldList));
                    }
                }
            }
        }

        if (queryBuilderList.size() > 0) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (QueryBuilder queryBuilder : queryBuilderList)
                boolQueryBuilder.must(queryBuilder);
            searchRequestBuilder.setQuery(boolQueryBuilder);
        }
        if (isSort) {
            for (TermsAggregationBuilder termsBuilder1 : aggregationBuilderList2) {
                if (isSort) {
                    searchRequestBuilder.addAggregation(termsBuilder1.order(Terms.Order.term(isAsc)).size(searchSize));
//                    searchRequestBuilder.addAggregation(termsBuilder1.order(Terms.Order.count()))
                } else {
                    searchRequestBuilder.addAggregation(termsBuilder1.size(searchSize));
                }
            }
        } else {
            for (AbstractAggregationBuilder pair : aggregationBuilderList) {
                searchRequestBuilder.addAggregation(pair);
            }
        }

        if (aggregationBuilderList.size() == 0) {
            for (SortBuilder sortBuilder : sortBuilderList)        //如果是分组排序  那么这个sortBuilderList就没有意义
                searchRequestBuilder.addSort(sortBuilder);
        }

        if (searchOffset >= 0)
            searchRequestBuilder.setFrom(searchOffset);
        if (searchSize > 0)
            searchRequestBuilder.setSize(searchSize);

        SearchResponse searchResponse = searchRequestBuilder.get();
        List<Object[]> resultList = new ArrayList<>();
        Boolean flag = false;
        int fieldIndex = 0;     //输出项分组变量的下标，返回结果时用
        List<RelDataTypeField> fl = getRowType2().getFieldList();
        for (int i = 0; i < outNames.size(); i++) {
            if (!outNames.get(i).contains("$")) {
                for (RelDataTypeField f : fl) {
                    String name = f.getName().toLowerCase();
                    if(StringUtils.equals(name,outNames.get(i).toLowerCase())){
                        flag = true;
                        fieldIndex = i;
                    }
                }
            }
        }

        //aggregation, only one row to return
        if (aggregationBuilderList.size() > 0) {
            if (!isGroup) {         //没分组的情况
                List<String> rowObj = new ArrayList<>();
                for (AbstractAggregationBuilder pair : aggregationBuilderList) {
                    String name = pair.getName();
                    NumericMetricsAggregation.SingleValue singleValue = searchResponse.getAggregations().get(name);
                    double remain = singleValue.value() % 1;
                    if (remain == 0)
//                    rowObj.add(singleValue.getValueAsString());
                        rowObj.add(String.valueOf((long) (singleValue.value())));
                    else
                        rowObj.add(singleValue.getValueAsString());
                }
                List<Pair<String, ElasticSearchFieldType>> pairList = Pair.zip(rowObj, typeList);
                if (oneColumFlag) {
                    singleResult.add(new TypeConverter.SingleColumnRowConverter(pairList).convertRow());
                } else {
                    result.add(new TypeConverter.ArrayRowConverter(pairList).convertRow());
                }
            } else {            //分组
                Terms terms = searchResponse.getAggregations().get(aggregationBuilderList.get(0).getName());
//                Iterator<Terms.Bucket> iterators = terms.getBuckets().iterator();
                Iterator<? extends Terms.Bucket> iterators = terms.getBuckets().iterator();
                while (iterators.hasNext()) {
                    List<String> rowObj = new ArrayList<String>();
                    Terms.Bucket bucket = iterators.next();
//                        Iterator<Aggregation> iterator = bucket.getAggregations().iterator();
                    for (String numeric : out) {
                        NumericMetricsAggregation.SingleValue a = bucket.getAggregations().get(numeric);
                        rowObj.add(String.valueOf((long) (a.value())));
                    }
                    if (flag) {
                        rowObj.add(fieldIndex, bucket.getKeyAsString());
                    }
                    List<Pair<String, ElasticSearchFieldType>> pairList = Pair.zip(rowObj, typeList);
                    if (oneColumFlag) {
                        singleResult.add(new TypeConverter.SingleColumnRowConverter(pairList).convertRow());
                    } else {
                        result.add(new TypeConverter.ArrayRowConverter(pairList).convertRow());
                    }
                }

                /*String name = aggregationBuilderList.get(0).getName();
                Terms terms = searchResponse.getAggregations().get(name);
//                Iterator<Terms.Bucket> iterators = terms.getBuckets().iterator();
                Iterator<? extends Terms.Bucket> iterators = terms.getBuckets().iterator();
                while (iterators.hasNext()) {
                    List<String> rowObj = new ArrayList<String>();
                    Terms.Bucket bucket = iterators.next();
//                        Iterator<Aggregation> iterator = bucket.getAggregations().iterator();
                    for (String numeric : out) {
                        NumericMetricsAggregation.SingleValue a = bucket.getAggregations().get(numeric);
                        if (StringUtils.equalsIgnoreCase("COUNT", numeric)) {
                            rowObj.add(String.valueOf((long) (a.value())));
                        } else {
                            rowObj.add(String.valueOf(a.value()));
                        }

                    }
                    if (flag) {
                        rowObj.add(fieldIndex, bucket.getKeyAsString());
                    }
                    List<Pair<String, ElasticSearchFieldType>> pairList = Pair.zip(rowObj, typeList);
                    if (oneColumFlag) {
                        singleResult.add(new TypeConverter.SingleColumnRowConverter(pairList).convertRow());
                    } else {
                        result.add(new TypeConverter.ArrayRowConverter(pairList).convertRow());
                    }
                }*/
            }
        } else {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                Map<String, Object> valueMap = hit.getSource();
//                List<RelDataTypeField> fieldList = rowType.getFieldList();
                List<String> rowObj = new ArrayList<>();
                for (String field : listField) {
//                    Object obj = valueMap.get(field.getName().toLowerCase());
                    Object obj = valueMap.get(field.toLowerCase());
                    if (obj != null)
                        rowObj.add(obj.toString());
                    else
                        rowObj.add(null);
                }

                if (rowObj.size() > 0) {
                    List<Pair<String, ElasticSearchFieldType>> pairList = Pair.zip(rowObj, typeList);
                    if (oneColumFlag) {
                        singleResult.add(new TypeConverter.SingleColumnRowConverter(pairList).convertRow());
                    } else {
                        result.add(new TypeConverter.ArrayRowConverter(pairList).convertRow());
                    }
                }
            }
        }
        return new TwoTuple<List<Object>, List<Object[]>>(singleResult, result);
    }

    public RelDataType[] getRowTypeFromEs(RelDataTypeFactory typeFactory) throws IOException {
        RelDataType[] relDataTypes = new RelDataType[2];
        RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        RelDataTypeFactory.FieldInfoBuilder builder1 = typeFactory.builder();
        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(esIndexName).get();
        MappingMetaData typeMapping = getMappingsResponse.getMappings().get(esIndexName).get(esTypeName);
        //{"person":{"properties":{"age":{"type":"integer"},"name":{"type":"text","fields":{"raw":{"type":"keyword"}}}}}}
        String json = typeMapping.source().string();
        Map<String, Object> map = new ObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> properties = ((Map<String, Map<String, Object>>) map.get(esTypeName)).get("properties");
        int index = 0;
        int index1 = 0;
        //(base-field-name, field-map)
        Stack<Pair<String, Map<String, Object>>> mapStack = new Stack<>();
        mapStack.push(Pair.of(null, properties));
        while (!mapStack.isEmpty()) {
            Pair<String, Map<String, Object>> pair = mapStack.pop();
            String baseFieldName = pair.left;
            for (Map.Entry<String, Object> entry : pair.right.entrySet()) {
                String name = entry.getKey().toUpperCase();
                if (baseFieldName != null) name = baseFieldName + "." + name;

                Map<String, Object> fieldMap = (Map<String, Object>) entry.getValue();
                String type = fieldMap.get("type") != null ? fieldMap.get("type").toString() : null;
                if (type == null)
                    throw new IllegalStateException(String.format("type of elasticsearch field '%s' is null", name));
                builder.add(new RelDataTypeFieldImpl(name, index++, typeFactory.createJavaType(ElasticSearchFieldType.of(type).getClazz())));

                if(!name.endsWith(".KEYWORD")){
                    builder1.add(new RelDataTypeFieldImpl(name, index1++, typeFactory.createJavaType(ElasticSearchFieldType.of(type).getClazz())));
                }
                //multi-field, that means containing 'fields' attribute
                Map<String, Object> moreFields = fieldMap.get("fields") != null ? (Map<String, Object>) fieldMap.get("fields") : null;
                if (moreFields != null) mapStack.push(Pair.of(name, moreFields));
            }
        }
        relDataTypes[0] = builder.build();
        relDataTypes[1] = builder1.build();
        return relDataTypes;
    }


    public String transFieldName(String fieldName, List<RelDataTypeField> fieldList) {
        for (RelDataTypeField f : fieldList) {
            if (StringUtils.equals(f.getName().toLowerCase(), fieldName.toLowerCase() + ".keyword")) {
                return fieldName.toLowerCase() + ".keyword";
            }
        }
        return fieldName;
    }
}
