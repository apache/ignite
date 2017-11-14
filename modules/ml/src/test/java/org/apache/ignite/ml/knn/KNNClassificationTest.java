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
import org.apache.ignite.ml.math.exceptions.UnresolvedClassException;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.ml.structures.LabeledDataset;


/** Tests behaviour of ColumnDecisionTreeTrainer. */
public class KNNClassificationTest extends BaseKNNTest {

    private static final String PATH_TO_FILE = "HERE";

    public void testBinaryClassificationTest() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
                new double[][]{
                        {1.0, 1.0},
                        {1.0, 2.0},
                        {2.0, 1.0},
                        {-1.0, -1.0},
                        {-1.0, -2.0},
                        {-2.0, -1.0}
                });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[]{1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);
               // = LabeledDataset.loadTxt(PATH_TO_FILE);

        KNNModel knnModel = new KNNModel(3, new EuclideanDistance(), KNNStrategy.SIMPLE,  training);
        Vector firstVector = new SparseBlockDistributedVector(new double[]{2.0, 2.0});
        assertEquals(knnModel.predict(firstVector), 1.0);
        Vector secondVector = new SparseBlockDistributedVector(new double[]{-2.0, -2.0});
        assertEquals(knnModel.predict(secondVector), 2.0);

    }

    public void testBinaryClassificationWithSmallestKTest() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
                new double[][]{
                        {1.0, 1.0},
                        {1.0, 2.0},
                        {2.0, 1.0},
                        {-1.0, -1.0},
                        {-1.0, -2.0},
                        {-2.0, -1.0}
                });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[]{1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);
        // = LabeledDataset.loadTxt(PATH_TO_FILE);

        KNNModel knnModel = new KNNModel(1, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector firstVector = new SparseBlockDistributedVector(new double[]{1.1, 1.1});
        assertEquals(knnModel.predict(firstVector), 1.0);
        Vector secondVector = new SparseBlockDistributedVector(new double[]{-2.1, -1.1});
        assertEquals(knnModel.predict(secondVector), 2.0);



    }

    // add test for weighted strategy

    public void testBinaryClassificationWithWeightedStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseBlockDistributedMatrix mtx = new SparseBlockDistributedMatrix(
                new double[][]{
                        {1.0, 1.0},
                        {1.0, 2.0},
                        {2.0, 1.0},
                        {-1, -1},
                        {-10.0, -20.0},
                        {-20.0, -10.0}
                });
        SparseBlockDistributedVector lbs = new SparseBlockDistributedVector(new double[]{1.0, 1.0, 1.0, 2.0, 2.0, 2.0});
        LabeledDataset<Matrix, Vector> training = new LabeledDataset<>(mtx, lbs);
        // = LabeledDataset.loadTxt(PATH_TO_FILE);

        KNNModel knnModel = new KNNModel(3, new EuclideanDistance(), KNNStrategy.WEIGHTED, training);
        Vector firstVector = new SparseBlockDistributedVector(new double[]{1.1, 1.1});
        assertEquals(knnModel.predict(firstVector), 1.0);
        Vector secondVector = new SparseBlockDistributedVector(new double[]{-1.1, -1.1});
        assertEquals(knnModel.predict(secondVector), 2.0); // should be 1.0 Debug it



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
