package com.dksou.essql.utils;

/**
 * Created by root on 2017/7/20.
 */
public enum  ElasticsearchFunction {
    LOWER,
    UPPER,
    TRIM,
    ABS,
    FLOOR,
    CEIL;

    ElasticsearchFunction() {

    }


    public static String handle(ElasticsearchFunction functionName , String field){
        String value = null;
       switch (functionName){
           case LOWER:
                value = field.toLowerCase();
               break;
           case UPPER:
               value= field.toUpperCase();
               break;
           case TRIM:
                value = field.trim();
               break;
           case ABS:
                value = String.valueOf(Math.abs(Double.parseDouble(field)));
               break;
           case CEIL:
               value = String.valueOf(Math.ceil(Double.parseDouble(field)));
               break;
           case FLOOR:
               value = String.valueOf(Math.floor(Double.parseDouble(field)));
               break;
           default:
               value = field;

        }
        return value;
    }

}
