package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@code CosineSimilarity}. */
public class CosineSimilarityTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

    /** */
    @Test
    public void cosineSimilarityDistance() {
        double expRes = 0.9449111825230682d;
        DenseVector a = new DenseVector(new double[] {1, 2, 3});
        double[] b = {1, 1, 4};

        DistanceMeasure distanceMeasure = new CosineSimilarity();

        assertEquals(expRes, distanceMeasure.compute(a, b), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(a, new DenseVector(b)), PRECISION);
    }
}