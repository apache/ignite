package org.apache.ignite.ml.tree.impurity;

import java.io.Serializable;

/**
 * Base interface for impurity measures that can be used in distributed decision tree algorithm.
 *
 * @param <T> Type of this impurity measure.
 */
public interface ImpurityMeasure<T extends ImpurityMeasure<T>> extends Comparable<T>, Serializable {
    /**
     * Calculates impurity measure as a single double value.
     *
     * @return Impurity measure value.
     */
    public double impurity();

    /**
     * Adds the given impurity to this.
     *
     * @param measure Another impurity.
     * @return Sum of this and the given impurity.
     */
    public T add(T measure);

    /**
     * Subtracts the given impurity for this.
     *
     * @param measure Another impurity.
     * @return Difference of this and the given impurity.
     */
    public T subtract(T measure);

    /** {@inheritDoc} */
    default public int compareTo(T o) {
        return Double.compare(impurity(), o.impurity());
    }
}
