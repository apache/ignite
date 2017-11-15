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

package org.apache.ignite.ml.knn;

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.knn.regression.KNNMultipleLinearRegression;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.exceptions.NullArgumentException;
import org.apache.ignite.ml.math.exceptions.SingularMatrixException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.regressions.AbstractMultipleLinearRegressionTest;
import org.apache.ignite.ml.regressions.OLSMultipleLinearRegression;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests for {@link OLSMultipleLinearRegression}.
 */
public class KNNMultipleLinearRegressionTest extends BaseKNNTest {
    /** */
    private double[] y;

    /** */
    private double[][] x;

    public static final String SEPARATOR = ",";

    /** */
    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

    }


    public void testSimpleRegressionWithOneNeighboor() {

        y = new double[] {11.0, 12.0, 13.0, 14.0, 15.0, 16.0};
        x = new double[6][];
        x[0] = new double[] {0, 0, 0, 0, 0};
        x[1] = new double[] {2.0, 0, 0, 0, 0};
        x[2] = new double[] {0, 3.0, 0, 0, 0};
        x[3] = new double[] {0, 0, 4.0, 0, 0};
        x[4] = new double[] {0, 0, 0, 5.0, 0};
        x[5] = new double[] {0, 0, 0, 0, 6.0};

        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(x);
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(y);
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);

        KNNMultipleLinearRegression knnModel = new KNNMultipleLinearRegression(1, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new SparseBlockDistributedVector(new double[]{0, 0, 0, 5.0, 0.0});
        System.out.println(knnModel.predict(vector));
        Assert.assertEquals(15, knnModel.predict(vector), 1E-12);

    }


    // TODO: repeat test with normalization
    public void testLongly() {

        y = new double[] {60323, 61122, 60171, 61187, 63221, 63639, 64989, 63761, 66019, 68169, 66513, 68655, 69564, 69331, 70551};
        x = new double[15][];
        x[0] = new double[] {83.0, 234289, 2356, 1590, 107608, 1947};
        x[1] = new double[] {88.5, 259426, 2325, 1456, 108632, 1948};
        x[2] = new double[] {88.2, 258054, 3682, 1616, 109773, 1949};
        x[3] = new double[] {89.5, 284599, 3351, 1650, 110929, 1950};
        x[4] = new double[] {96.2, 328975, 2099, 3099, 112075, 1951};
        x[5] = new double[] {98.1, 346999, 1932, 3594, 113270, 1952};
        x[6] = new double[] {99.0, 365385, 1870, 3547, 115094, 1953};
        x[7] = new double[] {100.0, 363112, 3578, 3350, 116219, 1954};
        x[8] = new double[] {101.2, 397469, 2904, 3048, 117388, 1955};
        x[9] = new double[] {108.4, 442769, 2936, 2798, 120445, 1957};
        x[10] = new double[] {110.8, 444546, 4681, 2637, 121950, 1958};
        x[11] = new double[] {112.6, 482704, 3813, 2552, 123366, 1959};
        x[12] = new double[] {114.2, 502601, 3931, 2514, 125368, 1960};
        x[13] = new double[] {115.7, 518173, 4806, 2572, 127852, 1961};
        x[14] = new double[] {116.9, 554894, 4007, 2827, 130081, 1962};


        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(x);
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(y);
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);

        KNNMultipleLinearRegression knnModel = new KNNMultipleLinearRegression(3, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new SparseBlockDistributedVector(new double[]{104.6, 419180, 2822, 2857, 118734, 1956});
        System.out.println(knnModel.predict(vector));
        Assert.assertEquals(67857, knnModel.predict(vector), 2000);

    }




    public void testLoadingMachineDatasetWithSimpleStrategyAndOneNeighboor(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset<Matrix, Vector> training = loadIrisDataset();

        KNNMultipleLinearRegression knnModel = new KNNMultipleLinearRegression(1, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new SparseBlockDistributedVector(new double[]{23,16000,32000,64,16,32});
        System.out.println(knnModel.predict(vector));
        Assert.assertEquals(381, knnModel.predict(vector), 1E-12);

    }

    public void testLoadingMachineDatasetWithSimpleStrategy(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset<Matrix, Vector> training = loadIrisDataset();

        KNNMultipleLinearRegression knnModel = new KNNMultipleLinearRegression(5, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new SparseBlockDistributedVector(new double[]{100,32000,64000,128,16,32});
        System.out.println(knnModel.predict(vector));
        Assert.assertEquals(900, knnModel.predict(vector), 100);

    }

    // add error calculation with different k
    // should be zero with k=1


    public void testCalculateAverageErrorOnMachineDatasetWithSimpleStrategy(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset<Matrix, Vector> training = loadIrisDataset();


        for (int amountOfNeighbours = 3; amountOfNeighbours < 10; amountOfNeighbours+=2) {
            System.out.println("Model initialized with k = " + amountOfNeighbours);

            KNNMultipleLinearRegression knnModel = new KNNMultipleLinearRegression(amountOfNeighbours, new EuclideanDistance(), KNNStrategy.SIMPLE, training);

            int rss = 0;
            for (int i = 0; i < training.rowSize(); i++){
                final Double prediction = knnModel.predict(training.getRow(i).vector());
                final double y = training.label(i);
                //System.out.println("Prediction is " + prediction + " y is " + y);
                if(prediction != y) rss+=Math.pow(prediction-y, 2.0); // maybe sqrt from this? and divide after that
            }


            System.out.println("RSS " + rss);
            System.out.println("Percentage of errors " + rss/(double)training.rowSize());
        }


    }



    private LabeledDataset<Matrix, Vector> loadIrisDataset() {
        String path = this.getClass().getClassLoader().getResource("knn/cleared_machines.txt").getPath();

        return LabeledDataset.loadTxt(path, SEPARATOR);
    }






/*    *//** *//*
    @Test(expected = MathIllegalArgumentException.class)
    public void cannotAddSampleDataWithSizeMismatch() {
        double[] y = new double[] {1.0, 2.0};
        double[][] x = new double[1][];
        x[0] = new double[] {1.0, 0};
        createRegression().newSampleData(new DenseLocalOnHeapVector(y), new DenseLocalOnHeapMatrix(x));
    }*/


}
