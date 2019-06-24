
package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import com.dksou.essql.ElasticsearchTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.List;


public class ElasticsearchProject extends Project implements ElasticsearchRelNode {
  public ElasticsearchProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                              List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    assert getConvention() == ElasticsearchRelNode.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override
  public Project copy(RelTraitSet relTraitSet, RelNode input, List<RexNode> projects,RelDataType relDataType) {
    return new ElasticsearchProject(getCluster(), traitSet, input, projects, relDataType);
  }

   public void implement(Implementor implementor) {
       implementor.visitChild(0, getInput());

       ElasticsearchTable esTable = implementor.getElasticsearchTable();

       final ElasticsearchRules.RexToElasticsearchTranslator translator =
               new ElasticsearchRules.RexToElasticsearchTranslator(
                       (JavaTypeFactory) getCluster().getTypeFactory(),
                       ElasticsearchRules.elasticsearchFieldNames(getInput().getRowType()));
       for (Pair<RexNode, String> pair : getNamedProjects()) {
           final String selectField = pair.right;
           final String originalName = pair.left.accept(translator);
           implementor.add(selectField);
       }
  }
}

// End ElasticsearchProject.java
