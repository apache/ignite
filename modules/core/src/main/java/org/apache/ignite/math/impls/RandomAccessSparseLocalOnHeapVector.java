package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;

/**
 * TODO: add description.
 */
public class RandomAccessSparseLocalOnHeapVector extends AbstractVector {
    public RandomAccessSparseLocalOnHeapVector(Vector vector) {
        super(vector);
    }

    @Override public Vector copy() {
        return new RandomAccessSparseLocalOnHeapVector(this);
    }

    @Override public Vector like(int crd) {
        return null;
    }

    @Override public Matrix likeMatrix(int rows, int cols) {
        return null;
    }

    @Override public Matrix toMatrix(boolean rowLike) {
        return null;
    }

    @Override public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
        return null;
    }
}
