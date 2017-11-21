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
import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.ml.structures.LabeledDataset;

/** Tests behaviour of KNNClassificationTest. */
public class KNNClassificationTest extends BaseKNNTest {

    public static final String SEPARATOR = "\t";

    public void testBinaryClassificationTest() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}
            });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);

        KNNModel knnModel = new KNNModel(3, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector firstVector = new SparseBlockDistributedVector(new double[] {2.0, 2.0});
        assertEquals(knnModel.predict(firstVector), 1.0);
        Vector secondVector = new SparseBlockDistributedVector(new double[] {-2.0, -2.0});
        assertEquals(knnModel.predict(secondVector), 2.0);

    }

    public void testBinaryClassificationWithSmallestKTest() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}
            });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);

        KNNModel knnModel = new KNNModel(1, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector firstVector = new SparseBlockDistributedVector(new double[] {1.1, 1.1});
        assertEquals(knnModel.predict(firstVector), 1.0);
        Vector secondVector = new SparseBlockDistributedVector(new double[] {-2.1, -1.1});
        assertEquals(knnModel.predict(secondVector), 2.0);

    }

    // add test for weighted strategy

    public void testBinaryClassificationFarPointsWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
            new double[][] {
                {10.0, 10.0},
                {10.0, 20.0},
                {-1, -1},
                {-2, -2},
                {-1.0, -2.0},
                {-2.0, -1.0}
            });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);

        KNNModel knnModel = new KNNModel(3, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new SparseBlockDistributedVector(new double[] {-1.01, -1.01});
        assertEquals(knnModel.predict(vector), 2.0);

    }

    public void testBinaryClassificationFarPointsWithWeightedStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
            new double[][] {
                {10.0, 10.0},
                {10.0, 20.0},
                {-1, -1},
                {-2, -2},
                {-1.0, -2.0},
                {-2.0, -1.0}
            });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);

        KNNModel knnModel = new KNNModel(3, new EuclideanDistance(), KNNStrategy.WEIGHTED, training);
        Vector vector = new SparseBlockDistributedVector(new double[] {-1.01, -1.01});
        assertEquals(knnModel.predict(vector), 1.0);

    }

    public void testLoadingIrisDatasetWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset<Matrix, Vector> training = loadIrisDataset();

        KNNModel knnModel = new KNNModel(7, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new SparseBlockDistributedVector(new double[] {5.15, 3.55, 1.45, 0.25});
        assertEquals(knnModel.predict(vector), 1.0);

    }

    public void testLoadingIrisDatasetWithWeightedStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset<Matrix, Vector> training = loadIrisDataset();

        KNNModel knnModel = new KNNModel(11, new EuclideanDistance(), KNNStrategy.WEIGHTED, training);
        Vector vector = new SparseBlockDistributedVector(new double[] {5.15, 3.55, 1.45, 0.25});
        assertEquals(knnModel.predict(vector), 1.0);

    }

    public void testLargeKValue() {
        // should fail with exception
    }

    public void testLoadingNonExistingFile() {

    }

    public void testLoadingFileWithIncorrectData() {

    }

    public void testLoadingFileWithMissedData() {

    }

    // TODO: to labeled dataset
    public void testDifferentSizesMatrixAndVector() {

    }

    // TODO: good idea for example http://www.rpubs.com/Drmadhu/IRISclassification
    // with splitting on test and train data
    public void testCalculateAverageErrorOnIrisDatasetWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset<Matrix, Vector> training = loadIrisDataset();

        for (int amountOfNeighbours = 1; amountOfNeighbours < 20; amountOfNeighbours += 2) {
            System.out.println("Model initialized with k = " + amountOfNeighbours);

            KNNModel knnModel = new KNNModel(amountOfNeighbours, new EuclideanDistance(), KNNStrategy.SIMPLE, training);

            int amountOfErrors = 0;
            for (int i = 0; i < training.rowSize(); i++) {
                if (knnModel.predict(training.getRow(i).vector()) != training.label(i))
                    amountOfErrors++;
            }

            System.out.println("Absolute amount of errors " + amountOfErrors);
            System.out.println("Percentage of errors " + amountOfErrors / (double)training.rowSize());
        }

    }

    private LabeledDataset<Matrix, Vector> loadIrisDataset() {
        String path = this.getClass().getClassLoader().getResource("knn/iris.txt").getPath();

        return LabeledDataset.loadTxt(path, SEPARATOR);
    }

    // add test with a few points with equal distance

    /*
            try {
            Vector thirdVector = new SparseBlockDistributedVector(new double[]{0.0, 0.0});
            knnModel.predict(thirdVector);
            fail("UnresolvedClassException expected");
        } catch (UnresolvedClassException e) {

        }
     */

}
