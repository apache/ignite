package org.apache.ignite.ml.optimization;

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

public class GradientDescentTest {

    /** */
    private static final double PRECISION = 1e-6;

    /** */
    @Test
    public void testBatch() {
        GradientDescent gd = new GradientDescent(null, null).withBatchFraction(0.5);
        Matrix inputs = new DenseLocalOnHeapMatrix(new double[][]{
            {1, 2},
            {3, 4},
            {5, 6},
            {7, 8}
        });
        Vector groundTruth = new DenseLocalOnHeapVector(new double[]{1, 2, 3, 4});

        IgniteBiTuple<Matrix, Vector> batch0 = gd.batch(inputs, groundTruth, 0);
        IgniteBiTuple<Matrix, Vector> batch1 = gd.batch(inputs, groundTruth, 1);
        IgniteBiTuple<Matrix, Vector> batch2 = gd.batch(inputs, groundTruth, 2);
        IgniteBiTuple<Matrix, Vector> batch3 = gd.batch(inputs, groundTruth, 3);

        TestUtils.assertEquals("Wrong batch (input)",
            new DenseLocalOnHeapMatrix(new double[][]{{1, 2}, {3, 4}}),
            batch0.get1(),
            PRECISION);
        TestUtils.assertEquals("Wrong batch ground truth",
            new DenseLocalOnHeapVector(new double[]{1, 2}),
            batch0.get2(),
            PRECISION);

        TestUtils.assertEquals("Wrong batch (input)",
            new DenseLocalOnHeapMatrix(new double[][]{{5, 6}, {7, 8}}),
            batch1.get1(),
            PRECISION);
        TestUtils.assertEquals("Wrong batch ground truth",
            new DenseLocalOnHeapVector(new double[]{3, 4}),
            batch1.get2(),
            PRECISION);

        TestUtils.assertEquals("Wrong batch (input)",
            new DenseLocalOnHeapMatrix(new double[][]{{1, 2}, {3, 4}}),
            batch2.get1(),
            PRECISION);
        TestUtils.assertEquals("Wrong batch ground truth",
            new DenseLocalOnHeapVector(new double[]{1, 2}),
            batch2.get2(),
            PRECISION);

        TestUtils.assertEquals("Wrong batch (input)",
            new DenseLocalOnHeapMatrix(new double[][]{{5, 6}, {7, 8}}),
            batch3.get1(),
            PRECISION);
        TestUtils.assertEquals("Wrong batch ground truth",
            new DenseLocalOnHeapVector(new double[]{3, 4}),
            batch3.get2(),
            PRECISION);
    }
}
