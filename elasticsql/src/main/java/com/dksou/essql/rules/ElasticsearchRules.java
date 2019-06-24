package com.dksou.essql.rules;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.AbstractList;
import java.util.List;

/**
 * Created by myy on 2017/6/29.
 */
public class ElasticsearchRules {
    public static final RelOptRule[] RULES = {
            ElasticsearchSortRule.INSTANCE,
            ElasticsearchFilterRule.INSTANCE,
            ElasticsearchAggregateRule.INSTANCE,
            ElasticsearchProjectRule.INSTANCE,
            ElasticsearchLimitRule.INSTANCE
//            ElasticsearchJoinRule.INSTANCE
    };

    static class RexToElasticsearchTranslator extends RexVisitorImpl<String> {
        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;

        protected RexToElasticsearchTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
            super(true);
            this.typeFactory = typeFactory;
            this.inFields = inFields;
        }

        @Override
        public String visitInputRef(RexInputRef inputRef) {
            return inFields.get(inputRef.getIndex());
        }
        }

    static String maybeQuote(String s) {
        if (!needsQuote(s)) {
            return s;
        }
        return quote(s);
    }
    static String quote(String s) {
        return "\"" + s + "\"";
    }
    private static boolean needsQuote(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (!Character.isJavaIdentifierPart(c)
                    || c == '$') {
                return true;
            }
        }
        return false;
    }
    public static List<String> elasticsearchFieldNames(final RelDataType rowType) {
        return SqlValidatorUtil.uniquify(
                new AbstractList<String>() {
                    @Override
                    public String get(int index) {
                        final String name = rowType.getFieldList().get(index).getName();
                        return name.startsWith("$") ? "_" + name.substring(2) : name;
                    }

                    @Override
                    public int size() {
                        return rowType.getFieldCount();
                    }
                });
    }

}
