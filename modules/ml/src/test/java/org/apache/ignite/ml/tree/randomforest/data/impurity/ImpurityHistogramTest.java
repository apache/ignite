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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.feature.Histogram;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ImpurityHistogram}.
 */
public class ImpurityHistogramTest {
    /** Count of classes. */
    private static final int COUNT_OF_CLASSES = 3;

    /** Lbl mapping. */
    static final Map<Double, Integer> lblMapping = new HashMap<>();

    /** Random generator. */
    protected Random rnd = new Random();

    static {
        for(int i = 0; i < COUNT_OF_CLASSES; i++)
            lblMapping.put((double)i, i);
    }

    /** */
    void checkBucketIds(Set<Integer> bucketIdsSet, Integer[] exp) {
        Integer[] bucketIds = new Integer[bucketIdsSet.size()];
        bucketIdsSet.toArray(bucketIds);
        assertArrayEquals(exp, bucketIds);
    }

    /** */
    void checkCounters(ObjectHistogram<BootstrappedVector> hist, double[] exp) {
        double[] counters = hist.buckets().stream().mapToDouble(x -> hist.getValue(x).get()).toArray();
        assertArrayEquals(exp, counters, 0.01);
    }

    /**
     * Generates random vector.
     *
     * @param isClassification Is classification.
     */
    BootstrappedVector randomVector(boolean isClassification) {
        double[] features = DoubleStream.generate(() -> rnd.nextDouble()).limit(2).toArray();
        int[] counters = IntStream.generate(() -> rnd.nextInt(10)).limit(1).toArray();
        double lbl = isClassification ? Math.abs(rnd.nextInt() % COUNT_OF_CLASSES) : rnd.nextDouble();
        return new BootstrappedVector(VectorUtils.of(features), lbl, counters);
    }

    /**
     * Check sums.
     *
     * @param exp Expected value.
     * @param partitions Partitions.
     */
    <T extends Histogram<BootstrappedVector, T>> void checkSums(T exp, List<T> partitions) {
        T leftSum = partitions.stream().reduce((x,y) -> x.plus(y)).get();
        T rightSum = partitions.stream().reduce((x,y) -> y.plus(x)).get();
        assertTrue(exp.isEqualTo(leftSum));
        assertTrue(exp.isEqualTo(rightSum));
    }
}
