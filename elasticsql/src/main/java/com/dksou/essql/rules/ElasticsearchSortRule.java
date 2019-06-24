package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchSortRule extends ConverterRule {
    static final ElasticsearchSortRule INSTANCE = new ElasticsearchSortRule();
    ElasticsearchSortRule(){
        super(LogicalSort.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchSortRule.class.getSimpleName());
    }

    public RelNode convert(RelNode relNode) {
        final LogicalSort sort = (LogicalSort) relNode;
        final RelTraitSet traitSet = sort.getTraitSet().replace(getOutTrait()).replace(sort.getCollation());
        return new ElasticsearchSort(relNode.getCluster(), traitSet,
                convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)), sort.getCollation(),
                sort.offset, sort.fetch);
    }
}
