package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;

/**
 * TODO: add description.
 */
public class RandomAccessSparseLocalOnHeapVector extends AbstractVector {
    /** */
    public RandomAccessSparseLocalOnHeapVector(Vector vector) {
        super(vector);
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return new RandomAccessSparseLocalOnHeapVector(this);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null;
    }
}
