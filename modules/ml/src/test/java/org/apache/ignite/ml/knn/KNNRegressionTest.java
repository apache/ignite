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
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.knn.classification.KNNStrategy;
import org.apache.ignite.ml.knn.regression.KNNRegressionModel;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link KNNRegressionTrainer}.
 */
@RunWith(Parameterized.class)
public class KNNRegressionTest {
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
    public void testSimpleRegressionWithOneNeighbour() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {11.0, 0, 0, 0, 0, 0});
        data.put(1, new double[] {12.0, 2.0, 0, 0, 0, 0});
        data.put(2, new double[] {13.0, 0, 3.0, 0, 0, 0});
        data.put(3, new double[] {14.0, 0, 0, 4.0, 0, 0});
        data.put(4, new double[] {15.0, 0, 0, 0, 5.0, 0});
        data.put(5, new double[] {16.0, 0, 0, 0, 0, 6.0});

        KNNRegressionTrainer trainer = new KNNRegressionTrainer();

        KNNRegressionModel knnMdl = (KNNRegressionModel) trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        ).withK(1)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector vector = new DenseVector(new double[] {0, 0, 0, 5.0, 0.0});
        System.out.println(knnMdl.apply(vector));
        Assert.assertEquals(15, knnMdl.apply(vector), 1E-12);
    }

    /** */
    @Test
    public void testLongly() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {60323, 83.0, 234289, 2356, 1590, 107608, 1947});
        data.put(1, new double[] {61122, 88.5, 259426, 2325, 1456, 108632, 1948});
        data.put(2, new double[] {60171, 88.2, 258054, 3682, 1616, 109773, 1949});
        data.put(3, new double[] {61187, 89.5, 284599, 3351, 1650, 110929, 1950});
        data.put(4, new double[] {63221, 96.2, 328975, 2099, 3099, 112075, 1951});
        data.put(5, new double[] {63639, 98.1, 346999, 1932, 3594, 113270, 1952});
        data.put(6, new double[] {64989, 99.0, 365385, 1870, 3547, 115094, 1953});
        data.put(7, new double[] {63761, 100.0, 363112, 3578, 3350, 116219, 1954});
        data.put(8, new double[] {66019, 101.2, 397469, 2904, 3048, 117388, 1955});
        data.put(9, new double[] {68169, 108.4, 442769, 2936, 2798, 120445, 1957});
        data.put(10, new double[] {66513, 110.8, 444546, 4681, 2637, 121950, 1958});
        data.put(11, new double[] {68655, 112.6, 482704, 3813, 2552, 123366, 1959});
        data.put(12, new double[] {69564, 114.2, 502601, 3931, 2514, 125368, 1960});
        data.put(13, new double[] {69331, 115.7, 518173, 4806, 2572, 127852, 1961});
        data.put(14, new double[] {70551, 116.9, 554894, 4007, 2827, 130081, 1962});

        KNNRegressionTrainer trainer = new KNNRegressionTrainer();

        KNNRegressionModel knnMdl = (KNNRegressionModel) trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector vector = new DenseVector(new double[] {104.6, 419180, 2822, 2857, 118734, 1956});
        System.out.println(knnMdl.apply(vector));
        Assert.assertEquals(67857, knnMdl.apply(vector), 2000);
    }

    /** */
    @Test
    public void testLonglyWithWeightedStrategy() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {60323, 83.0, 234289, 2356, 1590, 107608, 1947});
        data.put(1, new double[] {61122, 88.5, 259426, 2325, 1456, 108632, 1948});
        data.put(2, new double[] {60171, 88.2, 258054, 3682, 1616, 109773, 1949});
        data.put(3, new double[] {61187, 89.5, 284599, 3351, 1650, 110929, 1950});
        data.put(4, new double[] {63221, 96.2, 328975, 2099, 3099, 112075, 1951});
        data.put(5, new double[] {63639, 98.1, 346999, 1932, 3594, 113270, 1952});
        data.put(6, new double[] {64989, 99.0, 365385, 1870, 3547, 115094, 1953});
        data.put(7, new double[] {63761, 100.0, 363112, 3578, 3350, 116219, 1954});
        data.put(8, new double[] {66019, 101.2, 397469, 2904, 3048, 117388, 1955});
        data.put(9, new double[] {68169, 108.4, 442769, 2936, 2798, 120445, 1957});
        data.put(10, new double[] {66513, 110.8, 444546, 4681, 2637, 121950, 1958});
        data.put(11, new double[] {68655, 112.6, 482704, 3813, 2552, 123366, 1959});
        data.put(12, new double[] {69564, 114.2, 502601, 3931, 2514, 125368, 1960});
        data.put(13, new double[] {69331, 115.7, 518173, 4806, 2572, 127852, 1961});
        data.put(14, new double[] {70551, 116.9, 554894, 4007, 2827, 130081, 1962});

        KNNRegressionTrainer trainer = new KNNRegressionTrainer();

        KNNRegressionModel knnMdl = (KNNRegressionModel) trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(KNNStrategy.SIMPLE);

        Vector vector = new DenseVector(new double[] {104.6, 419180, 2822, 2857, 118734, 1956});
        System.out.println(knnMdl.apply(vector));
        Assert.assertEquals(67857, knnMdl.apply(vector), 2000);
    }
}
