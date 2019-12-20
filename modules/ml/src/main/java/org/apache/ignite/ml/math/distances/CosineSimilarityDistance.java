package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CosineSimilarityDistance implements DistanceMeasure {
    @Override
    public double compute(Vector a, Vector b) throws CardinalityException {
        return MatrixUtil.localCopyOf(a).dot(b) / (a.kNorm(2d) * b.kNorm(2d));
    }

    @Override
    public double compute(Vector a, double[] b) throws CardinalityException {
        return 0;
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
