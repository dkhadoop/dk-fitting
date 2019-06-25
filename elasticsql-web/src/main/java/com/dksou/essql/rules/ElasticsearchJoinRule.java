package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchJoinRule extends ConverterRule {
    static final ElasticsearchJoinRule INSTANCE = new ElasticsearchJoinRule();
    ElasticsearchJoinRule(){
        super(LogicalJoin.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchJoinRule.class.getSimpleName());
    }
    public RelNode convert(RelNode relNode) {
        LogicalJoin join = (LogicalJoin) relNode;
        List<RelNode> newInputs = new ArrayList<RelNode>();
        for (RelNode input : join.getInputs()) {
            if (!(input.getConvention().getName().equals(ElasticsearchRelNode.CONVENTION.getName()))) {
                input =
                        convert(
                                input,
                                input.getTraitSet()
                                        .replace(ElasticsearchRelNode.CONVENTION));
            }
            newInputs.add(input);
        }
        final RelOptCluster cluster = join.getCluster();
        final RelTraitSet traitSet =
                join.getTraitSet().replace(ElasticsearchRelNode.CONVENTION);
        final RelNode left = newInputs.get(0);
        final RelNode right = newInputs.get(1);

        return new ElasticsearchJoin(join.getCluster(), traitSet, left, right,
                join.getCondition(), join.getJoinType());
    }
}
