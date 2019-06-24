package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchAggregateRule extends ConverterRule {
    static final ElasticsearchAggregateRule INSTANCE = new ElasticsearchAggregateRule();
    ElasticsearchAggregateRule(){
        super(LogicalAggregate.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchAggregateRule.class.getSimpleName());
    }
    public RelNode convert(RelNode relNode) {
        LogicalAggregate aggregate = (LogicalAggregate) relNode;
        RelTraitSet traitSet = aggregate.getTraitSet().replace(getOutTrait());
        for (AggregateCall call : aggregate.getAggCallList())
        {
            switch (call.getAggregation().getKind())
            {
                case MIN:
                case MAX:
                case COUNT:
                case SUM:
                case AVG:break;
                default:return null;//doesn't match. aggregate rule doesn't fire
            }
        }
        return new ElasticsearchAggregate(aggregate.getCluster(), traitSet,
                convert(aggregate.getInput(), getOutTrait()), aggregate.indicator,
                aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
    }
}
