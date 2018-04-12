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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.KNNStrategy;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/** Tests behaviour of KNNClassificationTest. */
public class KNNClassificationTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** */
    public void testBinaryClassificationTest() {

        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {1.0, 1.0, 1.0});
        data.put(1, new double[] {1.0, 2.0, 1.0});
        data.put(2, new double[] {2.0, 1.0, 1.0});
        data.put(3, new double[] {-1.0, -1.0, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        KNNClassificationModel knnMdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> Arrays.copyOfRange(v, 0, v.length - 1),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector firstVector = new DenseLocalOnHeapVector(new double[] {2.0, 2.0});
        Assert.assertEquals(knnMdl.apply(firstVector), 1.0, PRECISION);
        Vector secondVector = new DenseLocalOnHeapVector(new double[] {-2.0, -2.0});
        Assert.assertEquals(knnMdl.apply(secondVector), 2.0, PRECISION);
    }

    /** */
    public void testBinaryClassificationWithSmallestKTest() {
        Map<Integer, double[]> data = new HashMap<>();

        data.put(0, new double[] {1.0, 1.0, 1.0});
        data.put(1, new double[] {1.0, 2.0, 1.0});
        data.put(2, new double[] {2.0, 1.0, 1.0});
        data.put(3, new double[] {-1.0, -1.0, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        KNNClassificationModel knnMdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> Arrays.copyOfRange(v, 0, v.length - 1),
            (k, v) -> v[2]
        ).withK(1)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector firstVector = new DenseLocalOnHeapVector(new double[] {2.0, 2.0});
        Assert.assertEquals(knnMdl.apply(firstVector), 1.0, PRECISION);
        Vector secondVector = new DenseLocalOnHeapVector(new double[] {-2.0, -2.0});
        Assert.assertEquals(knnMdl.apply(secondVector), 2.0, PRECISION);
    }

    /** */
    public void testBinaryClassificationFarPointsWithSimpleStrategy() {
        Map<Integer, double[]> data = new HashMap<>();

        data.put(0, new double[] {10.0, 10.0, 1.0});
        data.put(1, new double[] {10.0, 20.0, 1.0});
        data.put(2, new double[] {-1, -1, 1.0});
        data.put(3, new double[] {-2, -2, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        KNNClassificationModel knnMdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> Arrays.copyOfRange(v, 0, v.length - 1),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector vector = new DenseLocalOnHeapVector(new double[] {-1.01, -1.01});
        Assert.assertEquals(knnMdl.apply(vector), 2.0, PRECISION);
    }

    /** */
    public void testBinaryClassificationFarPointsWithWeightedStrategy() {
        Map<Integer, double[]> data = new HashMap<>();

        data.put(0, new double[] {10.0, 10.0, 1.0});
        data.put(1, new double[] {10.0, 20.0, 1.0});
        data.put(2, new double[] {-1, -1, 1.0});
        data.put(3, new double[] {-2, -2, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();

        KNNClassificationModel knnMdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> Arrays.copyOfRange(v, 0, v.length - 1),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.WEIGHTED);

        Vector vector = new DenseLocalOnHeapVector(new double[] {-1.01, -1.01});
        Assert.assertEquals(knnMdl.apply(vector), 1.0, PRECISION);
    }
}
