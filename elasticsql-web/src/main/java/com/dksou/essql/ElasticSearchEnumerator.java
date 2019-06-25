package com.dksou.essql;

import org.apache.calcite.linq4j.Enumerator;

/**
 * Created by myy on 2017/7/5.
 */
public class ElasticSearchEnumerator<E> implements Enumerator<E> {
    public E current() {
        return null;
    }

    public boolean moveNext() {
        return false;
    }

    public void reset() {

    }

    public void close() {

    }
}
