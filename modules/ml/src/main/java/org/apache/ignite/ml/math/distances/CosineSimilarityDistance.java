package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static java.lang.Math.sqrt;

public class CosineSimilarityDistance implements DistanceMeasure {
    @Override
    public double compute(Vector a, Vector b) throws CardinalityException {
        return MatrixUtil.localCopyOf(a).dot(b) / (a.kNorm(2d) * b.kNorm(2d));
    }
}
