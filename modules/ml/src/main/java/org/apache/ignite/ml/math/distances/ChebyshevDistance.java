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
}
