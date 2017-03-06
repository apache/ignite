package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.VectorStorage;
import org.apache.ignite.math.impls.matrix.FunctionMatrix;

import java.util.function.BiFunction;
import java.util.function.DoubleFunction;

/**
 * This class provides a helper implementation of the read-only implementation of {@link Vector}
 * interface to minimize the effort required to implement it.
 * Subclasses may override some of the implemented methods if a more
 * specific or optimized implementation is desirable.
 */
public abstract class AbstractReadonlyVector extends AbstractVector {
    /** */
    public AbstractReadonlyVector() {
        // No-op.
    }

    /**
     * @param readOnly Is read only.
     * @param sto Storage.
     */
    public AbstractReadonlyVector(boolean readOnly, VectorStorage sto) {
        super(readOnly, sto);
    }

    /** {@inheritDoc} */
    @Override public Matrix cross(Vector vec) {
        return new FunctionMatrix(size(), vec.size(),
            (row, col) -> vec.get(col) * get(row));
    }

    /** {@inheritDoc} */
    @Override public Matrix toMatrix(boolean rowLike) {
        return new FunctionMatrix(rowLike ? 1 : size(), rowLike ? size() : 1,
            (row, col) -> rowLike ? get(col) : get(row));
    }

    /** {@inheritDoc} */
    @Override public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
        return new FunctionMatrix(rowLike ? 1 : size() + 1, rowLike ? size() + 1 : 1, (row, col) -> {
            if (row == 0 && col == 0)
                return zeroVal;

            return rowLike ? get(col - 1) : get(row -1);
        });
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return this; // IMPL NOTE this exploits read-only feature of this type vector
    }

    /** {@inheritDoc} */
    @Override public Vector logNormalize() {
        return logNormalize(2.0, Math.sqrt(getLengthSquared()));
    }

    /** {@inheritDoc} */
    @Override public Vector logNormalize(double power) {
        return logNormalize(power, kNorm(power));
    }

    /** {@inheritDoc} */
    @Override public Vector map(DoubleFunction<Double> fun) {
        return new FunctionVector(size(), (i) -> fun.apply(get(i)));
    }

    /** {@inheritDoc} */
    @Override public Vector map(Vector vec, BiFunction<Double, Double, Double> fun) {
        checkCardinality(vec);

        return new FunctionVector(size(), (i) -> fun.apply(get(i), vec.get(i)));
    }

    /** {@inheritDoc} */
    @Override public Vector map(BiFunction<Double, Double, Double> fun, double y) {
        return new FunctionVector(size(), (i) -> fun.apply(get(i), y));
    }

    /** {@inheritDoc} */
    @Override public Vector divide(double x) {
        if (x == 1.0)
            return this;

        return new FunctionVector(size(), (i) -> get(i) / x);
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        return x == 0 ? new ConstantVector(size(), 0) : new FunctionVector(size(), (i) -> get(i) * x);
    }

    /**
     * @param power Power.
     * @param normLen Normalized length.
     * @return logNormalized value.
     */
    private Vector logNormalize(double power, double normLen) {
        assert !(Double.isInfinite(power) || power <= 1.0);

        double denominator = normLen * Math.log(power);

        return new FunctionVector(size(), (idx) -> Math.log1p(get(idx)) / denominator);
    }
}
