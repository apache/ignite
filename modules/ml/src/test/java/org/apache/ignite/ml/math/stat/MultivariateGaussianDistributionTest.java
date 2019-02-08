package org.apache.ignite.ml.math.stat;

import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link MultivariateGaussianDistribution}.
 */
public class MultivariateGaussianDistributionTest {
    /** */
    @Test
    public void testApply() {
        MultivariateGaussianDistribution distribution = new MultivariateGaussianDistribution(
            VectorUtils.of(1, 2),
            new DenseMatrix(new double[][] {new double[] {1, -0.5}, new double[] {-0.5, 1}})
        );

        Assert.assertEquals(0.183, distribution.prob(VectorUtils.of(1, 2)), 0.01);
        Assert.assertEquals(0.094, distribution.prob(VectorUtils.of(0, 2)), 0.01);
    }
}
