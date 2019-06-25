package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import com.dksou.essql.ElasticsearchTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchAggregate extends Aggregate implements ElasticsearchRelNode {
    protected ElasticsearchAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet,
                                     List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new ElasticsearchAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    }

    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        ElasticsearchTable esTable = implementor.getElasticsearchTable();
        List<AggregateCall> aggCallList = getAggCallList();
        List<RelDataTypeField> fieldList = esTable.getRowType().getFieldList();
        final List<String> inNames = ElasticsearchRules.elasticsearchFieldNames(getInput().getRowType());
        final List<String> outNames = ElasticsearchRules.elasticsearchFieldNames(getRowType());
        esTable.setOutNames(outNames);

        //        final List<String> inNames = ElasticsearchRules.elasticsearchFieldNames(getInput().getRowType());
        TermsAggregationBuilder groupAggregationBuilder = null;
        AggregationBuilder groupAggregationBuilder2 = null;     //防止出现group但没有sort出错的情况
        for(int i = 0 ;i<groupSet.cardinality();i++) {
            final String inName = inNames.get(groupSet.nth(i));

            groupAggregationBuilder = AggregationBuilders.terms(inName.toLowerCase()).field(transFieldName(ElasticsearchRules.maybeQuote(inName).toLowerCase(),fieldList));
            groupAggregationBuilder2 = AggregationBuilders.terms(inName).field(transFieldName(ElasticsearchRules.maybeQuote(inName),fieldList));
            esTable.setIsGroup(true);
        }
        List<String> out = new ArrayList<String>();   //聚合函数的名称
        for(AggregateCall call : aggCallList)
        {
            SqlAggFunction function = call.getAggregation();
            List<Integer> argList = call.getArgList();
            String functionName = function.getName();
            out.add(functionName);
            switch (function.getKind())
            {
                case MIN:
                    RelDataTypeField typeField = fieldList.get(argList.get(0));
                    if (groupAggregationBuilder == null) {
                        //min值 与 原字段值 的类型是一样的
                        esTable.addAggregationBuilder(AggregationBuilders.min(functionName).field(typeField.getName().toLowerCase()),
                                ((RelDataTypeFactoryImpl.JavaType) typeField.getType()).getJavaClass());
                    }else {
                        MinAggregationBuilder minAgg = AggregationBuilders.min(functionName).field(typeField.getName().toLowerCase());
//                        esTable.addAggregationBuilder(groupAggregationBuilder.subAggregation(minAgg), call.getType().getClass());
                        groupAggregationBuilder.subAggregation(minAgg);
                        groupAggregationBuilder2.subAggregation(minAgg);
                    }
                    break;
                case MAX:
                    //max值 与 原字段值 的类型是一样的
                    RelDataTypeField typeField1 = fieldList.get(argList.get(0));
                    if(groupAggregationBuilder == null) {
                        esTable.addAggregationBuilder(AggregationBuilders.max(functionName).field(typeField1.getName().toLowerCase()),
                                ((RelDataTypeFactoryImpl.JavaType) typeField1.getType()).getJavaClass());
                    }else {
                        groupAggregationBuilder.subAggregation(AggregationBuilders.max(functionName).field(typeField1.getName().toLowerCase()));
                    }
                    break;
                case COUNT:
                    if(groupAggregationBuilder == null) {
                        if (argList == null || argList.size() == 0)//count(*)
                            esTable.addAggregationBuilder(AggregationBuilders.count(functionName), Long.class);
                        else
                            esTable.addAggregationBuilder(AggregationBuilders.count(functionName).field(transFieldName(fieldList.get(argList.get(0)).getName().toLowerCase(), fieldList)), Long.class);
                    }else {
                        groupAggregationBuilder.subAggregation(AggregationBuilders.count(functionName).field(transFieldName(fieldList.get(argList.get(0)).getName().toLowerCase(), fieldList)));
                        groupAggregationBuilder2.subAggregation(AggregationBuilders.count(functionName).field(transFieldName(fieldList.get(argList.get(0)).getName().toLowerCase(), fieldList)));
                    }
                    break;
                case SUM:
                    if(groupAggregationBuilder == null) {
                        esTable.addAggregationBuilder(AggregationBuilders.sum(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase()), Double.class);
                    }else {
                        String s = fieldList.get(argList.get(0)).getName().toLowerCase();
                        SumAggregationBuilder sumAgg = AggregationBuilders.sum(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase());
                        groupAggregationBuilder.subAggregation(sumAgg);
                        groupAggregationBuilder2.subAggregation(sumAgg);
                    }
                    break;
                case AVG:
                    if(groupAggregationBuilder == null) {
                        esTable.addAggregationBuilder(AggregationBuilders.avg(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase()), Double.class);
                    }else {
                        AvgAggregationBuilder avgAgg = AggregationBuilders.avg(functionName).field(fieldList.get(argList.get(0)).getName().toLowerCase());
                        groupAggregationBuilder.subAggregation(avgAgg);
                        groupAggregationBuilder2.subAggregation(avgAgg);
                    }
                    break;
                default:break;
            }
        }
        if (groupAggregationBuilder != null) {
            esTable.addAggregationBuilder((AbstractAggregationBuilder) groupAggregationBuilder,String.class);
            esTable.addAggregationBuilderList2(groupAggregationBuilder);
        }
        esTable.setOut(out);
    }

    /**
     * 判断count(field) field是否需要添加keyword
     * @return
     */
    public String transFieldName(String fieldName,List<RelDataTypeField> fieldList){
        for (RelDataTypeField f:fieldList) {
            if (StringUtils.equals(f.getName().toLowerCase(),fieldName.toLowerCase()+".keyword")){
                return fieldName.toLowerCase()+".keyword";
            }
        }
        return fieldName;
    }
}
