package org.apache.ignite.ml.selection.mapper;

import java.io.Serializable;

@FunctionalInterface
public interface UniformMapper<K, V> extends Serializable {

    public double map(K key, V val);
}
