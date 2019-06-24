package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

/**
 * Created by myy on 2017/7/27.
 */
public class ElasticsearchCalcRule extends ConverterRule {
    static final ElasticsearchCalcRule INSTANCE = new ElasticsearchCalcRule();
    public ElasticsearchCalcRule() {
        super(LogicalCalc.class, Convention.NONE, ElasticsearchRelNode.CONVENTION, ElasticsearchCalcRule.class.getSimpleName());
    }


    public RelNode convert(RelNode rel) {
        final LogicalCalc calc = (LogicalCalc) rel;
        final RelTraitSet traitSet = calc.getTraitSet().replace(getOutTrait());
//        return new ElasticsearchFilter(rel.getCluster(), traitSet,
//                convert(calc.getInput(), getOutTrait()),
//                calc.getCondition());
        return null;
    }
}
