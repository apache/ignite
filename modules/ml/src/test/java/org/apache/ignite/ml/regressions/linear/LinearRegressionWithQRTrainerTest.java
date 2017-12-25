package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.junit.Test;

/**
 * Tests for {@link LinearRegressionWithQRTrainer}.
 */
public class LinearRegressionWithQRTrainerTest {

    /**
     * Tests trainer on artificial dataset with 10 observations described by 1 feature.
     */
    @Test
    public void testTrainOnArtificialDataset10x1() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression10x1;
        Trainer<LinearRegressionModel, Matrix> trainer = new LinearRegressionWithQRTrainer();
        LinearRegressionModel model = trainer.train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), 1e-5);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), 1e-5);
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset10x5() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression10x5;
        Trainer<LinearRegressionModel, Matrix> trainer = new LinearRegressionWithQRTrainer();
        LinearRegressionModel model = trainer.train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), 1e-5);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), 1e-5);
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x5() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression100x5;
        Trainer<LinearRegressionModel, Matrix> trainer = new LinearRegressionWithQRTrainer();
        LinearRegressionModel model = trainer.train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), 1e-5);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), 1e-5);
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 10 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x10() {
        ArtificialRegressionDatasets.Dataset dataset = ArtificialRegressionDatasets.regression100x10;
        Trainer<LinearRegressionModel, Matrix> trainer = new LinearRegressionWithQRTrainer();
        LinearRegressionModel model = trainer.train(dataset.getData());

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), 1e-5);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), 1e-5);
    }
}