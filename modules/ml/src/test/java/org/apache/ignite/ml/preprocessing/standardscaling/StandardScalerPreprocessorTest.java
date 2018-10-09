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
        double[][] inputData = new double[][] {
            {0, 2., 4., .1},
            {0, 1., -18., 2.2},
            {1, 4., 10., -.1},
            {1, 0., 22., 1.3}
        };
        double[] means = new double[] {0.5, 1.75, 4.5, 0.875};
        double[] sigmas = new double[] {0.5, 1.47901995, 14.51723114, 0.93374247};

        StandardScalerPreprocessor<Integer, Vector> preprocessor = new StandardScalerPreprocessor<>(
            means,
            sigmas,
            (k, v) -> v
        );

        double[][] expectedData = new double[][] {
            {-1., 0.16903085, -0.03444183, -0.82999331},
            {-1., -0.50709255, -1.54988233, 1.41902081},
            {1., 1.52127766, 0.37886012, -1.04418513},
            {1., -1.18321596, 1.20546403, 0.45515762}
        };

        for (int i = 0; i < inputData.length; i++)
            assertArrayEquals(expectedData[i], preprocessor.apply(i, VectorUtils.of(inputData[i])).asArray(), 1e-8);
    }
}
