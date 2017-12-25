package org.apache.ignite.ml.regressions.linear;

import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for {@link LinearRegressionWithSGDTrainer}.
 */
public class LinearRegressionWithSGDTrainerTest {

    /** */
    @Test
    public void testTrainWithoutIntercept() {
        Trainer<LinearRegressionModel, Matrix> tr = new LinearRegressionWithSGDTrainer(100, 1.0, 1e-10);
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0, 1.0},
            {4.0, 2.0}
        });
        LinearRegressionModel model = tr.train(data);
        TestUtils.assertEquals(4, model.apply(new DenseLocalOnHeapVector(new double[] {2})), 1e-10);
        TestUtils.assertEquals(6, model.apply(new DenseLocalOnHeapVector(new double[] {3})), 1e-10);
        TestUtils.assertEquals(8, model.apply(new DenseLocalOnHeapVector(new double[] {4})), 1e-10);
    }

    /** */
    @Test
    public void testTrainWithIntercept() {
        Trainer<LinearRegressionModel, Matrix> tr = new LinearRegressionWithSGDTrainer(100, 1.0, 1e-10);
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0, 0.0},
            {0.0, 1.0}
        });
        LinearRegressionModel model = tr.train(data);
        TestUtils.assertEquals(0.5, model.apply(new DenseLocalOnHeapVector(new double[] {0.5})), 1e-10);
        TestUtils.assertEquals(2, model.apply(new DenseLocalOnHeapVector(new double[] {-1})), 1e-10);
        TestUtils.assertEquals(-1, model.apply(new DenseLocalOnHeapVector(new double[] {2})), 1e-10);
    }

    /** */
    @Test
    public void testTrainOnDiabetesDataset() throws FileNotFoundException {
        Matrix data = loadDataset("datasets/regression/diabetes.csv", 442, 11);
        Trainer<LinearRegressionModel, Matrix> trainer = new LinearRegressionWithSGDTrainer(100_000, 1.0, 1e-10);
        LinearRegressionModel model = trainer.train(data);
        Vector expectedWeights = new DenseLocalOnHeapVector(new double[] {
            -10.01219782, -239.81908937, 519.83978679, 324.39042769, -792.18416163,
            476.74583782, 101.04457032, 177.06417623, 751.27932109, 67.62538639
        });
        double expectedIntercept = 152.13348416;

        TestUtils.assertEquals("Wrong weights", expectedWeights, model.getWeights(), 1e-5);
        TestUtils.assertEquals("Wrong intercept", expectedIntercept, model.getIntercept(), 1e-5);
    }

    /** */
    @Test
    @Ignore
    public void testTrainOnBostonDataset() throws FileNotFoundException {
        Matrix data = loadDataset("datasets/regression/boston.csv", 506, 14);
        Trainer<LinearRegressionModel, Matrix> trainer = new LinearRegressionWithSGDTrainer(100_000, 1.0, 1e-10);
        LinearRegressionModel model = trainer.train(data);
        Vector expectedWeights = new DenseLocalOnHeapVector(new double[] {
            -1.07170557e-01,   4.63952195e-02,   2.08602395e-02, 2.68856140e+00,  -1.77957587e+01,   3.80475246e+00,
            7.51061703e-04,  -1.47575880e+00,   3.05655038e-01, -1.23293463e-02,  -9.53463555e-01,   9.39251272e-03,
            -5.25466633e-01
        });
        double expectedIntercept = 36.4911032804;

        TestUtils.assertEquals("Wrong weights", expectedWeights, model.getWeights(), 1e-5);
        TestUtils.assertEquals("Wrong intercept", expectedIntercept, model.getIntercept(), 1e-5);
    }

    /** */
    private Matrix loadDataset(String fileName, int nobs, int nvars) throws FileNotFoundException {
        double[][] matrix = new double[nobs][nvars];
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
}
