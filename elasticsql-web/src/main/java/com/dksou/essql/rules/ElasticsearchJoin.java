package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by myy on 2017/7/14.
 */
public class ElasticsearchJoin extends Join  implements ElasticsearchRelNode {

    protected ElasticsearchJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition,  new HashSet<CorrelationId>(0), joinType);
    }

    @Override
    public Join copy(RelTraitSet relTraitSet, RexNode rexNode, RelNode relNode, RelNode relNode1, JoinRelType joinRelType, boolean b) {
        return new ElasticsearchJoin(getCluster(),relTraitSet,relNode,relNode1,rexNode,joinRelType);
    }

    public void implement(Implementor implementor) {
        implementor.visitChild(0, getLeft());
        implementor.visitChild(0, getRight());
        if (!getCondition().isA(SqlKind.EQUALS)) {
            throw new IllegalArgumentException("Only equi-join are supported");
        }
        List<RexNode> operands = ((RexCall) getCondition()).getOperands();
        if (operands.size() != 2) {
            throw new IllegalArgumentException("Only equi-join are supported");
        }
        List<Integer> leftKeys = new ArrayList<Integer>(1);
        List<Integer> rightKeys = new ArrayList<Integer>(1);
        List<Boolean> filterNulls = new ArrayList<Boolean>(1);
        RexNode rexNode = RelOptUtil.splitJoinCondition(getLeft(), getRight(), getCondition(), leftKeys, rightKeys,
                filterNulls);
        String leftRelAlias = implementor.getElasticsearchRelationAlias((ElasticsearchRelNode) getLeft());
        String rightRelAlias = implementor.getElasticsearchRelationAlias((ElasticsearchRelNode) getRight());
        String leftJoinFieldName = implementor.getFieldName((ElasticsearchRelNode) getLeft(), leftKeys.get(0));
        String rightJoinFieldName = implementor.getFieldName((ElasticsearchRelNode) getRight(), rightKeys.get(0));
        String result = implementor.getElasticsearchRelationAlias((ElasticsearchRelNode) getLeft())
                + " = JOIN " + leftRelAlias + " BY "+ leftJoinFieldName + ' ' + getElasticsearchJoinType() + ", " +
                rightRelAlias + " BY "+ rightJoinFieldName + ';';
        System.out.println("implementor = " + result);
    }
    @Override public RelOptTable getTable() {
        return getLeft().getTable();
    }

    private String getElasticsearchJoinType() {
        switch (getJoinType()) {
            case INNER:
                return "";
            default:
                return getJoinType().name();
        }
    }

}
