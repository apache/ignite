/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.regressions.linear;

import java.util.Scanner;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.junit.Test;

/**
 * Base class for all linear regression trainers.
 */
public class GenericLinearRegressionTrainerTest {
    /** */
    private final Trainer<LinearRegressionModel, Matrix> trainer;

    /** */
    private final IgniteFunction<double[][], Matrix> matrixCreator;

    /** */
    private final IgniteFunction<double[], Vector> vectorCreator;

    /** */
    private final double precision;

    /** */
    public GenericLinearRegressionTrainerTest(
        Trainer<LinearRegressionModel, Matrix> trainer,
        IgniteFunction<double[][], Matrix> matrixCreator,
        IgniteFunction<double[], Vector> vectorCreator,
        double precision) {
        this.trainer = trainer;
        this.matrixCreator = matrixCreator;
        this.vectorCreator = vectorCreator;
        this.precision = precision;
    }

    /**
     * Test trainer on regression model y = 2 * x.
     */
    @Test
    public void testTrainWithoutIntercept() {
        Matrix data = matrixCreator.apply(new double[][] {
            {2.0, 1.0},
            {4.0, 2.0}
        });
        LinearRegressionModel model = trainer.train(data);
        TestUtils.assertEquals(4, model.apply(vectorCreator.apply(new double[] {2})), precision);
        TestUtils.assertEquals(6, model.apply(vectorCreator.apply(new double[] {3})), precision);
        TestUtils.assertEquals(8, model.apply(vectorCreator.apply(new double[] {4})), precision);
    }

    /**
     * Test trainer on regression model y = -1 * x + 1.
     */
    @Test
    public void testTrainWithIntercept() {
        Matrix data = matrixCreator.apply(new double[][] {
            {1.0, 0.0},
            {0.0, 1.0}
        });
        LinearRegressionModel model = trainer.train(data);
        TestUtils.assertEquals(0.5, model.apply(vectorCreator.apply(new double[] {0.5})), precision);
        TestUtils.assertEquals(2, model.apply(vectorCreator.apply(new double[] {-1})), precision);
        TestUtils.assertEquals(-1, model.apply(vectorCreator.apply(new double[] {2})), precision);
    }

    /**
     * Test trainer on diabetes dataset.
     */
    @Test
    public void testTrainOnDiabetesDataset() {
        Matrix data = loadDataset("datasets/regression/diabetes.csv", 442, 10);
        LinearRegressionModel model = trainer.train(data);
        Vector expectedWeights = vectorCreator.apply(new double[] {
            -10.01219782, -239.81908937, 519.83978679, 324.39042769, -792.18416163,
            476.74583782, 101.04457032, 177.06417623, 751.27932109, 67.62538639
        });
        double expectedIntercept = 152.13348416;

        TestUtils.assertEquals("Wrong weights", expectedWeights, model.getWeights(), precision);
        TestUtils.assertEquals("Wrong intercept", expectedIntercept, model.getIntercept(), precision);
    }

    /**
     * Test trainer on boston dataset.
     */
    @Test
    public void testTrainOnBostonDataset() {
        Matrix data = loadDataset("datasets/regression/boston.csv", 506, 13);
        LinearRegressionModel model = trainer.train(data);
        Vector expectedWeights = vectorCreator.apply(new double[] {
            -1.07170557e-01, 4.63952195e-02, 2.08602395e-02, 2.68856140e+00, -1.77957587e+01, 3.80475246e+00,
            7.51061703e-04, -1.47575880e+00, 3.05655038e-01, -1.23293463e-02, -9.53463555e-01, 9.39251272e-03,
            -5.25466633e-01
        });
        double expectedIntercept = 36.4911032804;

        TestUtils.assertEquals("Wrong weights", expectedWeights, model.getWeights(), precision);
        TestUtils.assertEquals("Wrong intercept", expectedIntercept, model.getIntercept(), precision);
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 1 feature.
     */
    @Test
    public void testTrainOnArtificialDataset10x1() {
        ArtificialRegressionDatasets.TestDataset dataset = ArtificialRegressionDatasets.regression10x1;
        LinearRegressionModel model = trainer.train(matrixCreator.apply(dataset.getData()));

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), precision);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), precision);
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset10x5() {
        ArtificialRegressionDatasets.TestDataset dataset = ArtificialRegressionDatasets.regression10x5;
        LinearRegressionModel model = trainer.train(matrixCreator.apply(dataset.getData()));

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), precision);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), precision);
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x5() {
        ArtificialRegressionDatasets.TestDataset dataset = ArtificialRegressionDatasets.regression100x5;
        LinearRegressionModel model = trainer.train(matrixCreator.apply(dataset.getData()));

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), precision);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), precision);
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 10 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x10() {
        ArtificialRegressionDatasets.TestDataset dataset = ArtificialRegressionDatasets.regression100x10;
        LinearRegressionModel model = trainer.train(matrixCreator.apply(dataset.getData()));

        TestUtils.assertEquals("Wrong weights", dataset.getExpectedWeights(), model.getWeights(), precision);
        TestUtils.assertEquals("Wrong intercept", dataset.getExpectedIntercept(), model.getIntercept(), precision);
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
        return matrixCreator.apply(matrix);
    }
}
