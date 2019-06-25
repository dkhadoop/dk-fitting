package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Created by myy on 2017/7/24.
 */
public  class ElasticsearchLimitRule extends RelOptRule {

    public static final ElasticsearchLimitRule INSTANCE = new ElasticsearchLimitRule();
    public ElasticsearchLimitRule() {
        super(operand(EnumerableLimit.class,operand(ElasticsearchToEnumerableConverter.class,any())),"ElasticsearchLimitRule");
    }

    public RelNode convert(EnumerableLimit limit){
        RelTraitSet traitSet = limit.getTraitSet().replace(ElasticsearchRelNode.CONVENTION);
        return new ElasticsearchLimit(limit.getCluster(),traitSet,convert(limit.getInput(),ElasticsearchRelNode.CONVENTION),limit.offset,limit.fetch);
    }
    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        EnumerableLimit limit = relOptRuleCall.rel(0);
        RelNode convert = convert(limit);
        if(convert != null){
            relOptRuleCall.transformTo(convert);
        }
    }
}