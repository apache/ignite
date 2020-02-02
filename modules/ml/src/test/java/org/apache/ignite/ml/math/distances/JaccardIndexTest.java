package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@code JaccardIndex}. */
public class JaccardIndexTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

    /** */
    @Test
    public void jaccardIndex() {
        double expRes =  0.2;
        double[] data2 = new double[] {2.0, 1.0, 0.0};
        Vector v1 = new DenseVector(new double[] {0.0, 0.0, 0.0});
        Vector v2 = new DenseVector(data2);

        DistanceMeasure distanceMeasure = new JaccardIndex();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }
}
