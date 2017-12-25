package org.apache.ignite.ml.regressions.linear;

import java.util.Scanner;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Base class for all linear regression trainers.
 */
public abstract class AbstractLinearRegressionTrainerTest {

    /** */
    private static final double DEFAULT_PRECISION = 1e-6;

    /** */
    abstract Trainer<LinearRegressionModel, Matrix> trainer();

    /**
     * Test trainer on regression model y = 2 * x.
     */
    @Test
    public void testTrainWithoutIntercept() {
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0, 1.0},
            {4.0, 2.0}
        });
        LinearRegressionModel model = trainer().train(data);
        TestUtils.assertEquals(4, model.apply(new DenseLocalOnHeapVector(new double[] {2})), getPrecision());
        TestUtils.assertEquals(6, model.apply(new DenseLocalOnHeapVector(new double[] {3})), getPrecision());
        TestUtils.assertEquals(8, model.apply(new DenseLocalOnHeapVector(new double[] {4})), getPrecision());
    }

    /**
     * Test trainer on regression model y = -1 * x + 1.
     */
    @Test
    public void testTrainWithIntercept() {
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0, 0.0},
            {0.0, 1.0}
        });
        LinearRegressionModel model = trainer().train(data);
        TestUtils.assertEquals(0.5, model.apply(new DenseLocalOnHeapVector(new double[] {0.5})), getPrecision());
        TestUtils.assertEquals(2, model.apply(new DenseLocalOnHeapVector(new double[] {-1})), getPrecision());
        TestUtils.assertEquals(-1, model.apply(new DenseLocalOnHeapVector(new double[] {2})), getPrecision());
    }

    /**
     * Test trainer on diabetes dataset.
     */
    @Test
    public void testTrainOnDiabetesDataset() {
        Matrix data = loadDataset("datasets/regression/diabetes.csv", 442, 10);
        LinearRegressionModel model = trainer().train(data);
        Vector expectedWeights = new DenseLocalOnHeapVector(new double[] {
            -10.01219782, -239.81908937, 519.83978679, 324.39042769, -792.18416163,
            476.74583782, 101.04457032, 177.06417623, 751.27932109, 67.62538639
        });
        double expectedIntercept = 152.13348416;

        TestUtils.assertEquals("Wrong weights", expectedWeights, model.getWeights(), getPrecision());
        TestUtils.assertEquals("Wrong intercept", expectedIntercept, model.getIntercept(), getPrecision());
    }

    /**
     * Test trainer on boston dataset.
     */
    @Test
    public void testTrainOnBostonDataset() {
        Matrix data = loadDataset("datasets/regression/boston.csv", 506, 13);
        LinearRegressionModel model = trainer().train(data);
        Vector expectedWeights = new DenseLocalOnHeapVector(new double[] {
            -1.07170557e-01,   4.63952195e-02,   2.08602395e-02, 2.68856140e+00,  -1.77957587e+01,   3.80475246e+00,
            7.51061703e-04,  -1.47575880e+00,   3.05655038e-01, -1.23293463e-02,  -9.53463555e-01,   9.39251272e-03,
            -5.25466633e-01
        });
        double expectedIntercept = 36.4911032804;

        TestUtils.assertEquals("Wrong weights", expectedWeights, model.getWeights(), getPrecision());
        TestUtils.assertEquals("Wrong intercept", expectedIntercept, model.getIntercept(), getPrecision());
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 1 feature.
     */
    @Test
    public void testTrainOnArtificialDataset10x1() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression10x1;
        LinearRegressionModel model = trainer().train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), getPrecision());
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), getPrecision());
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset10x5() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression10x5;
        LinearRegressionModel model = trainer().train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), getPrecision());
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), getPrecision());
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x5() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression100x5;
        LinearRegressionModel model = trainer().train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), getPrecision());
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), getPrecision());
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 10 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x10() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression100x10;
         LinearRegressionModel model = trainer().train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), getPrecision());
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), getPrecision());
    }

    /**
     * Loads dataset file and returns corresponding matrix.
     *
     * @param fileName Dataset file name
     * @param nobs Number of observations
     * @param nvars Number of features
     * @return Data matrix
     */
    private Matrix loadDataset(String fileName, int nobs, int nvars) {
        double[][] matrix = new double[nobs][nvars + 1];
        Scanner scanner = new Scanner(this.getClass().getClassLoader().getResourceAsStream(fileName));
        int i = 0;
        while (scanner.hasNextLine()) {
            String row = scanner.nextLine();
            int j = 0;
            for (String feature : row.split(",")) {
                matrix[i][j] = Double.parseDouble(feature);
                j++;
            }
            i++;
        }
        return new DenseLocalOnHeapMatrix(matrix);
    }

    /** */
    double getPrecision() {
        return DEFAULT_PRECISION;
    }
}
