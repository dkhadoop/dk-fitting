package com.dksou.essql;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by myy on 2017/6/29.
 */
public interface ElasticsearchRelNode extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("ELASTICSEARCH", ElasticsearchRelNode.class);

    class Implementor {
        public int offset = 0;
        public int fetch = 10;
        public RelOptTable table;
        public ElasticsearchTable elasticsearchTable;

        final List<Pair<String, String>> listPair =
                new ArrayList<Pair<String, String>>();
        public void addPair(String field, String expr) {
            listPair.add(Pair.of(field, expr));
        }
        final List<String> listField =
                new ArrayList<String>();
        public void add(String field) {
            listField.add(field);
        }
        public List<String> getListField(){
            return listField;
        }
        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((ElasticsearchRelNode) input).implement(this);
        }

        public RelOptTable getTable() {
            return table;
        }

        public ElasticsearchTable getElasticsearchTable() {
            return elasticsearchTable;
        }

        public String getElasticsearchRelationAlias(RelNode input) {
            return getTableName(input);
        }

        public String getTableName(RelNode input) {
            RelOptTable table = input.getInputs().get(0).getTable();
            List<String> qualifiedName1 = table.getQualifiedName();
            String s = qualifiedName1.get(qualifiedName1.size() - 1);
            return qualifiedName1.get(qualifiedName1.size() - 1);
        }

        public String getFieldName(RelNode input, int index) {
            return input.getRowType().getFieldList().get(index).getName();
        }
    }
}
