package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchFilterRule extends ConverterRule {
    static final ElasticsearchFilterRule INSTANCE = new ElasticsearchFilterRule();
    ElasticsearchFilterRule(){
        super(LogicalFilter.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchFilterRule.class.getSimpleName());
    }
    public RelNode convert(RelNode relNode) {
        final LogicalFilter filter = (LogicalFilter) relNode;
        final RelTraitSet traitSet = filter.getTraitSet().replace(getOutTrait());
        return new ElasticsearchFilter(relNode.getCluster(), traitSet,
                convert(filter.getInput(), getOutTrait()),
                filter.getCondition());
    }
}
