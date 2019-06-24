package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchToEnumerableConverterRule extends ConverterRule{
    public static final ElasticsearchToEnumerableConverterRule INSTANCE = new ElasticsearchToEnumerableConverterRule();
    ElasticsearchToEnumerableConverterRule(){
        super(RelNode.class, ElasticsearchRelNode.CONVENTION, EnumerableConvention.INSTANCE,
                ElasticsearchToEnumerableConverterRule.class.getSimpleName());
    }

    public RelNode convert(RelNode rel) {
        RelTraitSet traitSet = rel.getTraitSet().replace(getOutTrait());
        return new ElasticsearchToEnumerableConverter(rel.getCluster(), traitSet, rel);
    }
}
