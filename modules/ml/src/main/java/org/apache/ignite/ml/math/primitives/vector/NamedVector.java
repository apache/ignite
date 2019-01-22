package org.apache.ignite.ml.math.primitives.vector;

import java.util.Set;

/**
 * A named vector interface based on {@link Vector}. In addition to base vector functionality allows to set and get
 * elements using names as index.
 */
public interface NamedVector extends Vector {
    /**
     * Returns element with specified string index.
     *
     * @param idx Element string index.
     * @return Element value.
     */
    public double get(String idx);

    /**
     * Sets element with specified string index and value.
     *
     * @param idx Element string index.
     * @param val Element value.
     * @return
     */
    public NamedVector set(String idx, double val);

    /**
     * Returns list of string indexes used in this vector.
     *
     * @return List of string indexes used in this vector.
     */
    public Set<String> getKeys();
}
