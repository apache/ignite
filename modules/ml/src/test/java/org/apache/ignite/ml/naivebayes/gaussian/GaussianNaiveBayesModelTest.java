package org.apache.ignite.ml.naivebayes.gaussian;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Assert;
import org.junit.Test;

/** Created by Ravil on 08/09/2018. */
public class GaussianNaiveBayesModelTest {
    private static final double PRECISION = 1e-6;

    @Test
    public void testPredictWithMultiClasses() {
        double[][] means = new double[][] {
            {5.855, 176.25, 11.25},
            {5.4175, 132.5, 7.5},
        };

        double[][] variances = new double[][] {
            {3.5033E-2, 1.2292E2, 9.1667E-1},
            {9.7225E-2, 5.5833E2, 1.6667},
        };
        Vector probabilities = new DenseVector(new double[] {.5, .5});
        GaussianNaiveBayesModel mdl = new GaussianNaiveBayesModel(means, variances, probabilities);

        Vector observation = new DenseVector(new double[] {6, 130, 8});
//        Double[] expected = {6.1984E-9, 5.3778E-4};
        Integer expected =1;
        Assert.assertEquals(expected, mdl.apply(observation));
    }

}
