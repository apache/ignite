package org.apache.ignite.ml.preprocessing.standardscaling;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link StandardScalerPreprocessor}.
 */
public class StandardScalerPreprocessorTest {

    @Test
    public void apply_() {
        double[][] inputData = new double[][]{
            {2., 4., 1.},
            {1., 8., 22.},
            {4., 10., 100.},
            {0., 22., 300.}
        };
        double[] means= new double[]{1,2,3};
        double[] variances = new double[]{1,2,3};

        StandardScalerPreprocessor<Integer, Vector> preprocessor = new StandardScalerPreprocessor<>(
            means,
            variances,
            (k,v)-> v
        );

        double[][] expectedData = new double[][]{
            {2. / 4, (4. - 4.) / 18.,  0.},
            {1. / 4, (8. - 4.) / 18.,  (22. - 1.) / 299.},
            {1.,     (10. - 4.) / 18., (100. - 1.) / 299.},
            {0.,     (22. - 4.) / 18., (300. - 1.) / 299.}
        };

        for (int i = 0; i < inputData.length; i++)
            assertArrayEquals(expectedData[i], preprocessor.apply(i, VectorUtils.of(inputData[i])).asArray(), 1e-8);
    }
}
