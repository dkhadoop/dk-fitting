package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

/**
 * Created by myy on 2017/7/1.
 */
public class ElasticsearchProjectRule extends ConverterRule {
    public static final ElasticsearchProjectRule INSTANCE = new ElasticsearchProjectRule();

    private ElasticsearchProjectRule() {
        super(LogicalProject.class, Convention.NONE, ElasticsearchRelNode.CONVENTION,
                ElasticsearchProjectRule.class.getSimpleName());
    }

    @Override public RelNode convert(RelNode relNode) {
        final LogicalProject project = (LogicalProject) relNode;
        final RelTraitSet traitSet = project.getTraitSet().replace(getOutTrait());
        return new ElasticsearchProject(project.getCluster(), traitSet,
                convert(project.getInput(), getOutTrait()), project.getProjects(), project.getRowType());
    }
}
