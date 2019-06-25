package com.dksou.essql.rules;

import com.dksou.essql.ElasticsearchRelNode;
import com.dksou.essql.ElasticsearchTable;
import com.dksou.essql.utils.ElasticSearchFieldType;
import com.dksou.essql.utils.TwoTuple;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchToEnumerableConverter  extends ConverterImpl implements EnumerableRel {
    public ElasticsearchToEnumerableConverter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child) {
        super(cluster, ConventionTraitDef.INSTANCE, traitSet, child);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ElasticsearchToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final ElasticsearchRelNode.Implementor esImplementor = new ElasticsearchRelNode.Implementor();
        esImplementor.visitChild(0, getInput());
        List<String> listField = esImplementor.getListField();
        List<String> listField2 = new ArrayList<String>();
        for (String field : listField){                            //数据处理函数expr$0 转为lower.fieldName
                listField2.add(field);
        }
        ElasticsearchTable esTable = esImplementor.elasticsearchTable;

        final RelDataType rowType2 = esImplementor.elasticsearchTable.getRowType2();
        final List<String> fieldNames1  = rowType2.getFieldNames();          //得到输出顺序
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType2,
                pref.prefer(JavaRowFormat.ARRAY));



        final RelDataType rowType = esImplementor.elasticsearchTable.getRowType();
        PhysType physType2 = PhysTypeImpl.of(implementor.getTypeFactory(), rowType,
                pref.prefer(JavaRowFormat.ARRAY));
//        esTable.setOutNames(fieldNames1);
        List<String> fieldNames = ElasticsearchRules.elasticsearchFieldNames(rowType2);
        List<ElasticSearchFieldType> typeList = new ArrayList<ElasticSearchFieldType>();
        for(int i = 0 ; i < fieldNames.size(); i++){

            Class type = physType.fieldClass(i);
            String typeName = type.toString().substring(type.toString().lastIndexOf(".") + 1).toLowerCase();
            if("integer".equals(typeName)){
                typeName = "int";
            }
            typeList.add(ElasticSearchFieldType.of(typeName));
        }
        boolean oneColumFlag = false;
        if(typeList.size() == 1){
            oneColumFlag = true;
        }
        TwoTuple<List<Object>, List<Object[]>> listTwoTuple = esTable.find(typeList,listField,oneColumFlag);
//        List<Object[]> resultList = esTable.find(typeList, oneColumFlag);
        ConstantExpression constant = oneColumFlag ? Expressions.constant(listTwoTuple.first.toArray()) : Expressions.constant(listTwoTuple.second.toArray());
        Result result = implementor.result(physType2,
                Blocks.toBlock(Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, constant)));
        return result;
    }

}
