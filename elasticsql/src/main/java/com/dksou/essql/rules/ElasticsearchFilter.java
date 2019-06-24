package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import com.dksou.essql.ElasticsearchTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchFilter extends Filter implements ElasticsearchRelNode {
    public ElasticsearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new ElasticsearchFilter(getCluster(), traitSet, input, condition);
    }

    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        ElasticsearchTable esTable = implementor.getElasticsearchTable();
        QueryBuilder queryBuilder = translate();
        esTable.addQueryBuilder(queryBuilder);

    }

    public QueryBuilder translate() {
        final List<RexNode> orNodes = RelOptUtil.disjunctions(condition);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (RexNode node : orNodes)
            boolQueryBuilder.should(translateAnd(node));
        return boolQueryBuilder;
    }

    private static Object literalValue(RexLiteral literal) {
//        System.out.println(literal.getValue());
        return literal.getValue2();  //literal.getValue2()
    }


    private static Object literalValueWithDouble(RexLiteral literal){
        return literal.getValue();
    }

    private final Multimap<String, Pair<SqlKind, RexLiteral>> multimap = HashMultimap.create();

    private QueryBuilder translateAnd(RexNode node0) {
        multimap.clear();
        for (RexNode node : RelOptUtil.conjunctions(node0))
            translateMatch(node);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        ArrayList<String> fieldNameList = new ArrayList<>();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for(RelDataTypeField field : fieldList){
            fieldNameList.add(field.getName().toLowerCase());
        }

        for (Map.Entry<String, Collection<Pair<SqlKind, RexLiteral>>> entry : multimap.asMap().entrySet())
        {
            for (Pair<SqlKind, RexLiteral> pair : entry.getValue())
            {
                String key = entry.getKey().toLowerCase();

                /**
                 * 修改where后的条件字段 没加keyword
                 */
                if (fieldNameList.contains(key + ".keyword")) {
                    key = key + ".keyword";
                }


                Object value;
                /**
                 * 解决where条件参数为double类型活float类型 3.49
                 */
                if ("DECIMAL".equals(pair.right.getTypeName().toString())) {
                    value = literalValueWithDouble(pair.right).toString();
                }else {
                    value = literalValue(pair.right);
                }

                switch (pair.left) {
                    case EQUALS:
                        boolQueryBuilder.must(QueryBuilders.termQuery(key, value));
                        break;
                    case LESS_THAN:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(null).to(value, false));
                        break;
                    case LESS_THAN_OR_EQUAL:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(null).to(value, true));
                        break;
                    case NOT_EQUALS:
                        boolQueryBuilder.mustNot(QueryBuilders.termQuery(key, value));
                        break;
                    case GREATER_THAN:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(value, false).to(null));
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        boolQueryBuilder.must(QueryBuilders.rangeQuery(key).from(value, true).to(null));
                        break;
                    case LIKE:
                        if(!(value instanceof String))
                            throw new IllegalArgumentException("invalid type of value " + value + ". String type is expected!");
                        String _value = (String) value;
                        StringBuilder stringBuilder = new StringBuilder(_value);
                        //wildcard replace. The _ in sql "equals to" ? in lucene
                        //The % in sql "equals to" * in lucene
                        Pattern pattern = Pattern.compile("(\\w_)|(\\w%)");
                        Matcher matcher = pattern.matcher(_value);
                        while (matcher.find())
                        {
                            String group = matcher.group();
                            if(group.endsWith("_"))
                                stringBuilder.replace(matcher.start(), matcher.end(), group.replace("_", "?"));
                            else if(group.endsWith("%"))
                                stringBuilder.replace(matcher.start(), matcher.end(), group.replace("%", "*"));
                        }

                        //SQL use \_ and \% to escape.
                        //but elasticsearch doesn't need it
                        pattern = Pattern.compile("(\\\\_)|(\\\\%)");
                        matcher = pattern.matcher(stringBuilder.toString());
                        while (matcher.find())
                        {
                            String group = matcher.group();
                            stringBuilder.replace(matcher.start(), matcher.end(), group.replace("\\", ""));
                        }
                        _value = stringBuilder.toString();
                        //optimize. using prefix query instead of wildcard query
                        if(_value.matches("^\\w+\\*$"))
                            boolQueryBuilder.must(QueryBuilders.prefixQuery(key, _value.substring(0, _value.indexOf("*"))));
                        else boolQueryBuilder.must(QueryBuilders.wildcardQuery(key, _value));
                        break;
                    case IS_NULL:
                        boolQueryBuilder.mustNot(QueryBuilders.existsQuery(key));
                        break;
                    case IS_NOT_NULL:
                        boolQueryBuilder.must(QueryBuilders.existsQuery(key));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation of " + node0);
                }
            }
        }
        return boolQueryBuilder;
    }

    private Void translateMatch(RexNode node) {
        switch (node.getKind()) {
            case EQUALS:
            case NOT_EQUALS:
            case LIKE:
            case IS_NULL:
            case IS_NOT_NULL:
                return translateBinary(node.getKind(), node.getKind(), (RexCall) node);
            case LESS_THAN:
                return translateBinary(SqlKind.LESS_THAN, SqlKind.GREATER_THAN, (RexCall) node);
            case LESS_THAN_OR_EQUAL:
                return translateBinary(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL, (RexCall) node);
            case GREATER_THAN:
                return translateBinary(SqlKind.GREATER_THAN, SqlKind.LESS_THAN, (RexCall) node);
            case GREATER_THAN_OR_EQUAL:
                return translateBinary(SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL, (RexCall) node);
            default:
                throw new UnsupportedOperationException("cannot translate " + node);
        }
    }

    /**
     * binary operator translation.
     * Reverse the position of operands if necessary. For example, "10 <= age" to "age >= 10"
     * We expect the right operand to be literal
     */
    private Void translateBinary(SqlKind operator, SqlKind reverseOperator, RexCall call) {
        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);
        boolean res = translateBinary(operator, left, right);
        if (res) return null;
        res = translateBinary(reverseOperator, right, left);
        if (res) return null;
        throw new UnsupportedOperationException("cannot translate operator " + operator + " call " + call);
    }

    private boolean translateBinary(SqlKind operator, RexNode left, RexNode right) {
        if (right.getKind() != SqlKind.LITERAL) return false;
        final RexLiteral rightLiteral = (RexLiteral) right;
        switch (left.getKind())
        {
            case INPUT_REF:
                String name = getRowType().getFieldNames().get(((RexInputRef) left).getIndex());
                translateOp(operator, name, rightLiteral);
                return true;
            case CAST:
                return translateBinary(operator, ((RexCall) left).operands.get(0), right);
            default: return false;
        }
    }

    private void translateOp(SqlKind op, String name, RexLiteral right) {
        multimap.put(name, Pair.of(op, right));
    }

    private String isItem(RexCall call) {
        if (call.getOperator() != SqlStdOperatorTable.ITEM) {
            return null;
        }
        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);

        if (op0 instanceof RexInputRef
                && ((RexInputRef) op0).getIndex() == 0
                && op1 instanceof RexLiteral
                && ((RexLiteral) op1).getValue2() instanceof String) {
            return (String) ((RexLiteral) op1).getValue2();
        }
        return null;
    }

}
