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
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.exceptions.knn.SmallTrainingDatasetSizeException;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;

/** Tests behaviour of KNNClassificationTest. */
public class KNNClassificationTest extends BaseKNNTest {
    /** */
    public void testBinaryClassificationTest() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        LabeledDataset training = new LabeledDataset(mtx, lbs);

        KNNModel knnMdl = new KNNModel(3, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector firstVector = new DenseLocalOnHeapVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.predict(firstVector), 1.0);
        Vector secondVector = new DenseLocalOnHeapVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.predict(secondVector), 2.0);
    }

    /** */
    public void testBinaryClassificationWithSmallestKTest() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        LabeledDataset training = new LabeledDataset(mtx, lbs);

        KNNModel knnMdl = new KNNModel(1, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector firstVector = new DenseLocalOnHeapVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.predict(firstVector), 1.0);
        Vector secondVector = new DenseLocalOnHeapVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.predict(secondVector), 2.0);
    }

    /** */
    public void testBinaryClassificationFarPointsWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double[][] mtx =
            new double[][] {
                {10.0, 10.0},
                {10.0, 20.0},
                {-1, -1},
                {-2, -2},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};
        LabeledDataset training = new LabeledDataset(mtx, lbs);

        KNNModel knnMdl = new KNNModel(3, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new DenseLocalOnHeapVector(new double[] {-1.01, -1.01});
        assertEquals(knnMdl.predict(vector), 2.0);
    }

    /** */
    public void testBinaryClassificationFarPointsWithWeightedStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double[][] mtx =
            new double[][] {
                {10.0, 10.0},
                {10.0, 20.0},
                {-1, -1},
                {-2, -2},
                {-1.0, -2.0},
                {-2.0, -1.0}
            };
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};
        LabeledDataset training = new LabeledDataset(mtx, lbs);

        KNNModel knnMdl = new KNNModel(3, new EuclideanDistance(), KNNStrategy.WEIGHTED, training);
        Vector vector = new DenseLocalOnHeapVector(new double[] {-1.01, -1.01});
        assertEquals(knnMdl.predict(vector), 1.0);
    }

    /** */
    public void testPredictOnIrisDataset() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset training = loadDatasetFromTxt(KNN_IRIS_TXT, false);

        KNNModel knnMdl = new KNNModel(7, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
        Vector vector = new DenseLocalOnHeapVector(new double[] {5.15, 3.55, 1.45, 0.25});
        assertEquals(knnMdl.predict(vector), 1.0);
    }

    /** */
    public void testLargeKValue() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        double[][] mtx =
            new double[][] {
                {10.0, 10.0},
                {10.0, 20.0},
                {-1, -1},
                {-2, -2},
                {-1.0, -2.0},
                {-2.0, -1.0}
            };
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};
        LabeledDataset training = new LabeledDataset(mtx, lbs);

        try {
            new KNNModel(7, new EuclideanDistance(), KNNStrategy.SIMPLE, training);
            fail("SmallTrainingDatasetSizeException");
        }
        catch (SmallTrainingDatasetSizeException e) {
            return;
        }
        fail("SmallTrainingDatasetSizeException");
    }
}
