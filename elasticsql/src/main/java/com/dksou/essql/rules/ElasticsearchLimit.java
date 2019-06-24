package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import com.dksou.essql.ElasticsearchTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Created by root on 17/5/17.
 */
public class ElasticsearchLimit extends SingleRel implements ElasticsearchRelNode{
    public final RexNode offset;
    public final RexNode fetch;
    protected ElasticsearchLimit(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode offset, RexNode fetch) {
        super(cluster, traits, input);
        this.offset = offset;
        this.fetch = fetch;
        assert getConvention() == input.getConvention();
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                                RelMetadataQuery mq) {
        // We do this so we get the limit for free
        return planner.getCostFactory().makeZeroCost();
    }

    @Override public ElasticsearchLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
        return new ElasticsearchLimit(getCluster(), traitSet, sole(newInputs), offset, fetch);
    }

    public void implement(Implementor implementor) {
        RelNode input = getInput();
        implementor.visitChild(0, getInput());
        ElasticsearchTable esTable = implementor.getElasticsearchTable();
        if (offset != null) {
            implementor.offset = RexLiteral.intValue(offset);
            esTable.setSearchOffset(RexLiteral.intValue(offset));
        }

        if (fetch != null) {
            implementor.fetch = RexLiteral.intValue(fetch);
            esTable.setSearchSize(RexLiteral.intValue(fetch));
        }
    }

    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }
}
