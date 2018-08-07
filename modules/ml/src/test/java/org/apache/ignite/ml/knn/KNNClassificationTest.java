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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.KNNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertEquals;

/** Tests behaviour of KNNClassificationTest. */
@RunWith(Parameterized.class)
public class KNNClassificationTest {
    /** Number of parts to be tested. */
    private static final int[] partsToBeTested = new int[] {1, 2, 3, 4, 5, 7, 100};

    /** Number of partitions. */
    @Parameterized.Parameter
    public int parts;

    /** Parameters. */
    @Parameterized.Parameters(name = "Data divided on {0} partitions, training with batch size {1}")
    public static Iterable<Integer[]> data() {
        List<Integer[]> res = new ArrayList<>();

        for (int part : partsToBeTested)
            res.add(new Integer[] {part});

        return res;
    }

    /** */
    @Test
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.apply(firstVector), 1.0);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.apply(secondVector), 2.0);
    }

    /** */
    @Test
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(1)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.apply(firstVector), 1.0);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.apply(secondVector), 2.0);
    }

    /** */
    @Test
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(knnMdl.apply(vector), 2.0);
    }

    /** */
    @Test
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
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.WEIGHTED);

        Vector vector = new DenseVector(new double[] {-1.01, -1.01});
        assertEquals(knnMdl.apply(vector), 1.0);
    }
}
