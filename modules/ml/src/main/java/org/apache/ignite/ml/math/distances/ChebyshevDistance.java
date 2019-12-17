package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static java.lang.Math.abs;

/**
 * Calculates the {@code max(x_i - y_i)} (Chebyshev) distance between two points.
 */
public class ChebyshevDistance implements DistanceMeasure {

    /** {@inheritDoc} */
    @Override public double compute(Vector a, Vector b) throws CardinalityException {
        return MatrixUtil.localCopyOf(a).minus(b).foldMap(Math::max, Math::abs, 0d);
    }

    /** {@inheritDoc} */
    @Override public double compute(Vector a, double[] b) throws CardinalityException {
        double res = 0d;

        for (int i = 0; i < b.length; i++)
            res = Math.max(abs(b[i] - a.get(i)), res);
        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op
    }
}
