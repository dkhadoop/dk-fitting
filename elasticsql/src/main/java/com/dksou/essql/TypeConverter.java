package com.dksou.essql;

import com.dksou.essql.utils.ElasticSearchFieldType;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by myy on 2017/7/5.
 */
public class TypeConverter {
    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;
    static {
        TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }

    abstract static class RowConverter<E> {
        abstract E convertRow();
        protected Object convert(ElasticSearchFieldType type, String string) {
            if (type == null) {
                return string;
            }
            switch (type) {
                case BOOLEAN:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    return Boolean.parseBoolean(string);
                case BYTE:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    return Byte.parseByte(string);
                case SHORT:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    return Short.parseShort(string);
                case INT:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    Double aDouble = Double.valueOf(string);              //防止出现10.0转int出现的转换异常
                    return Integer.parseInt(String.valueOf(aDouble.intValue()));
//                    return Integer.parseInt(string);
                case LONG:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    return Long.parseLong(string);
                case FLOAT:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    return Float.parseFloat(string);
                case DOUBLE:
                    if (string == null || string.length() == 0  ) {
                        return null;
                    }
                    return Double.parseDouble(string);
                case DATE:
                    if (string == null || string.length() == 0  ){
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse(string);
                        return new java.sql.Date(date.getTime());
                    } catch (ParseException e) {
                        return null;
                    }
                case TIME:
                    if (string == null || string.length() == 0  ){
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse(string);
                        return new java.sql.Time(date.getTime());
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    if (string == null || string.length() == 0  ){
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return new java.sql.Timestamp(date.getTime());
                    } catch (ParseException e) {
                        return null;
                    }
                case STRING:
                case TEXT:
                case KEYWORD:
                default:
                    return string;
            }
        }
    }

    static class ArrayRowConverter extends RowConverter<Object[]> {
        private final List<Pair<String, ElasticSearchFieldType>> pairList;
        ArrayRowConverter(List<Pair<String, ElasticSearchFieldType>> pairList) {
            this.pairList = pairList;
        }
        public Object[] convertRow() {
            return convertNormalRow();
        }
        public Object[] convertNormalRow() {
            final Object[] objects = new Object[pairList.size()];
            for(int i = 0; i < pairList.size(); i++){
                Pair<String, ElasticSearchFieldType> pair = pairList.get(i);
                String value = pair.left;
                ElasticSearchFieldType type = pair.right;
                objects[i] = convert(type,value);
            }
            return objects;
        }
    }

     static class SingleColumnRowConverter extends RowConverter {
        private final List<Pair<String, ElasticSearchFieldType>> pairList;
         SingleColumnRowConverter(List<Pair<String, ElasticSearchFieldType>> pairList) {
            this.pairList = pairList;
        }
        Object convertRow() {
            Pair<String, ElasticSearchFieldType> pair = pairList.get(0);
            String value = pair.left;
            ElasticSearchFieldType type = pair.right;
            return convert(type,value);
        }
    }
}
