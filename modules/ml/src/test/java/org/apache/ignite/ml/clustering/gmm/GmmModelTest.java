package org.apache.ignite.ml.clustering.gmm;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.junit.Assert;
import org.junit.Test;

public class GmmModelTest {
    /** */
    @Test
    public void testTrivialCasesWithOneComponent() {
        Vector mean = VectorUtils.of(1., 2.);
        DenseMatrix covariance = MatrixUtil.fromList(Arrays.asList(
            VectorUtils.of(1, -0.5),
            VectorUtils.of(-0.5, 1)),
            true
        );

        GmmModel gmm = new GmmModel(
            VectorUtils.of(1.0),
            Collections.singletonList(new MultivariateGaussianDistribution(mean, covariance))
        );

        Assert.assertEquals(2, gmm.dimension());
        Assert.assertEquals(1, gmm.countOfComponents());
        Assert.assertEquals(VectorUtils.of(1.), gmm.componentsProbs());
        Assert.assertEquals(0., gmm.predict(mean), 0.01);
        Assert.assertEquals(1, gmm.likelihood(mean).size());
        Assert.assertEquals(0.183, gmm.likelihood(mean).get(0), 0.01);
        Assert.assertEquals(0.183, gmm.prob(mean), 0.01);
    }

    /** */
    @Test
    public void testTwoComponents() {
        Vector mean1 = VectorUtils.of(1., 2.);
        DenseMatrix covariance1 = MatrixUtil.fromList(Arrays.asList(
            VectorUtils.of(1, -0.25),
            VectorUtils.of(-0.25, 1)),
            true
        );

        Vector mean2 = VectorUtils.of(2., 1.);
        DenseMatrix covariance2 = MatrixUtil.fromList(Arrays.asList(
            VectorUtils.of(1, 0.5),
            VectorUtils.of(0.5, 1)),
            true
        );

        GmmModel gmm = new GmmModel(
            VectorUtils.of(0.5, 0.5),
            Arrays.asList(
                new MultivariateGaussianDistribution(mean1, covariance1),
                new MultivariateGaussianDistribution(mean2, covariance2)
            )
        );

        Assert.assertEquals(0., gmm.predict(mean1), 0.01);
        Assert.assertEquals(1., gmm.predict(mean2), 0.01);
        Assert.assertEquals(0., gmm.predict(VectorUtils.of(1.5, 1.5)), 0.01);
        Assert.assertEquals(1., gmm.predict(VectorUtils.of(3., 0.)), 0.01);
    }
}
