package com.romix.scala.collection.concurrent;

import java.util.*;

/**
 * Helper class simulating a tuple of 2 elements in Scala
 *
 * @param <K>
 * @param <V>
 * @author Roman Levenstein <romixlev@gmail.com>
 */
public class Pair<K, V> implements Map.Entry<K, V> {

    final K k;
    final V v;

    Pair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    @Override
    public K getKey() {
        // TODO Auto-generated method stub
        return k;
    }

    @Override
    public V getValue() {
        // TODO Auto-generated method stub
        return v;
    }

    @Override
    public V setValue(V value) {
        throw new RuntimeException("Operation not supported");
    }

}
