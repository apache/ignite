package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import java.util.Objects;

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
        assert a.size() == b.size();
        IgniteDoubleFunction<Double> fun = value -> Math.pow(Math.abs(value), p);

        Double result = MatrixUtil.localCopyOf(a).minus(b).foldMap(PLUS, fun, 0d);
        return Math.pow(result, 1/p);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MinkowskiDistance that = (MinkowskiDistance) o;
        return Double.compare(that.p, p) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(p);
    }
}
