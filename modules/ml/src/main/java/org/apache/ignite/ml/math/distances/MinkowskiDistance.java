package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.apache.ignite.ml.math.functions.Functions.PLUS;

/**
 * Calculates the L<sub>p</sub> (Minkowski) distance between two points.
 */
public class MinkowskiDistance implements DistanceMeasure {
    /** Serializable version identifier. */
    private static final long serialVersionUID = 1717556319784040040L;

    /** Distance paramenter. */
    private final double p;

    /** @param p norm */
    public MinkowskiDistance(double p) {
        this.p = p;
    }

    /** {@inheritDoc} */
    @Override public double compute(Vector a, Vector b) throws CardinalityException {
        IgniteDoubleFunction<Double> fun = value -> Math.pow(Math.abs(value), p);

        Double result = MatrixUtil.localCopyOf(a).minus(b).foldMap(PLUS, fun, 0d);
        return Math.pow(result, 1/p);
    }

    /** {@inheritDoc} */
    @Override public double compute(Vector a, double[] b) throws CardinalityException {
        double res = 0.0;

        for (int i = 0; i < b.length; i++)
            res += Math.pow(Math.abs(b[i] - a.get(i)), p);

        return Math.pow(res, 1/p);
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
