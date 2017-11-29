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

package org.apache.ignite.ml.knn.samples;

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.knn.BaseKNNTest;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.knn.regression.KNNMultipleLinearRegression;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.junit.Assert;

/**
 * Tests for {@link KNNMultipleLinearRegression}.
 */
public class KNNMultipleLinearRegressionSample extends BaseKNNTest {
    /** */
    public static final String KNN_CLEARED_MACHINES_TXT = "knn/cleared_machines.txt";

    /** */
    private double[] y;

    /** */
    private double[][] x;

    /** */
    public void testLoadingMachineDatasetWithSimpleStrategyAndOneNeighboor() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset training = loadDatasetFromTxt(KNN_CLEARED_MACHINES_TXT, true);

        KNNMultipleLinearRegression knnMdl = new KNNMultipleLinearRegression(1, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new DenseLocalOnHeapVector(new double[] {23, 16000, 32000, 64, 16, 32});
        System.out.println(knnMdl.predict(vector));
        Assert.assertEquals(381, knnMdl.predict(vector), 1E-12);

    }

    /** */
    public void testLoadingMachineDatasetWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset training = loadDatasetFromTxt(KNN_CLEARED_MACHINES_TXT, true);

        KNNMultipleLinearRegression knnMdl = new KNNMultipleLinearRegression(5, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new DenseLocalOnHeapVector(new double[] {100, 32000, 64000, 128, 16, 32});
        System.out.println(knnMdl.predict(vector));
        Assert.assertEquals(900, knnMdl.predict(vector), 100);

    }


    /** Ready for sample */
    public void testCalculateAverageErrorOnMachineDatasetWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset training = loadDatasetFromTxt(KNN_IRIS_TXT, false);

        for (int amountOfNeighbours = 3; amountOfNeighbours < 10; amountOfNeighbours += 2) {
            System.out.println("Model initialized with k = " + amountOfNeighbours);

            KNNMultipleLinearRegression knnMdl = new KNNMultipleLinearRegression(amountOfNeighbours, new EuclideanDistance(), KNNStrategy.SIMPLE, training);

            int rss = 0;
            for (int i = 0; i < training.rowSize(); i++) {
                final Double prediction = knnMdl.predict(training.getRow(i).features());
                final double y = training.label(i);
                //System.out.println("Prediction is " + prediction + " y is " + y);
                if (prediction != y)
                    rss += Math.pow(prediction - y, 2.0); // maybe sqrt from this? and divide after that
            }

            System.out.println("RSS " + rss);
            System.out.println("Percentage of errors " + rss / (double)training.rowSize());
        }

    }
}
