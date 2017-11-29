package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** */
public class DistanceTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

    /** */
    private Vector v1;

    /** */
    private Vector v2;

    /** */
    @Before
    public void setup() {
        v1 = new DenseLocalOnHeapVector(new double[] {0.0, 0.0, 0.0});
        v2 = new DenseLocalOnHeapVector(new double[] {2.0, 1.0, 0.0});
    }

    /** */
    @Test
    public void euclideanDistance() throws Exception {

        double expRes = Math.pow(5, 0.5);

        DistanceMeasure distanceMeasure = new EuclideanDistance();

        Assert.assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void manhattanDistance() throws Exception {
        double expRes = 3;

        DistanceMeasure distanceMeasure = new ManhattanDistance();

        Assert.assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void hammingDistance() throws Exception {
        double expRes = 2;

        DistanceMeasure distanceMeasure = new HammingDistance();

        Assert.assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

}
