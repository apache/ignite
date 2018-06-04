package org.apache.ignite.ml.selection.split.mapper;

import java.io.Serializable;

/**
 * Interface for util mappers that maps a key-value pair to a point on the segment (0, 1).
 *
 * @param <K> Type of a key.
 * @param <V> Type of a value.
 */
@FunctionalInterface
public interface UniformMapper<K, V> extends Serializable {
    /**
     *
     * @param key Key.
     * @param val Value.
     * @return Point on the segment (0, 1).
     */
    public double map(K key, V val);
}
