package com.dksou.essql.utils;

/**
 * Created by myy on 2017/7/5.
 */
public class TwoTuple<A,B> {
    public final A first;
    public final B second;
    public TwoTuple(A  a,B b){
        first=a;
        second=b;
    }
    public String toString(){
        return "("+first+", " +second+")";
    }
}
