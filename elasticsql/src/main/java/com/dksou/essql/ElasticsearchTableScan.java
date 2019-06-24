package com.dksou.essql;

import com.dksou.essql.rules.ElasticsearchRules;
import com.dksou.essql.rules.ElasticsearchToEnumerableConverterRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchTableScan  extends TableScan implements ElasticsearchRelNode{

    private final ElasticsearchTable elasticsearchTable;
    private final RelDataType projectRowType;

    public ElasticsearchTableScan(RelOptCluster cluster, RelTraitSet relTraits, RelOptTable relOptTable, ElasticsearchTable elasticsearchTable,
                                  RelDataType projectRowType) {
        super(cluster, relTraits, relOptTable);
        this.elasticsearchTable = elasticsearchTable;
        this.projectRowType = projectRowType;
        assert elasticsearchTable != null;
        assert getConvention() == ElasticsearchRelNode.CONVENTION;
    }

    public void implement(Implementor implementor) {
        implementor.elasticsearchTable = elasticsearchTable;
        implementor.table = table;
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(ElasticsearchToEnumerableConverterRule.INSTANCE);
        for (RelOptRule rule: ElasticsearchRules.RULES) {
            planner.addRule(rule);
        }

//        planner.removeRule(AggregateExpandDistinctAggregatesRule.INSTANCE);
//        planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    }
}
