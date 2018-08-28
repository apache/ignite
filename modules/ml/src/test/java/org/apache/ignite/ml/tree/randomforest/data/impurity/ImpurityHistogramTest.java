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

public class ImpurityHistogramTest {
    protected static final int COUNT_OF_CLASSES = 3;
    protected static final Map<Double, Integer> lblMapping = new HashMap<>();
    protected Random rnd = new Random();

    static {
        for(int i = 0; i < COUNT_OF_CLASSES; i++)
            lblMapping.put((double)i, i);
    }

    protected void checkBucketIds(Set<Integer> bucketIdsSet, Integer[] expected) {
        Integer[] bucketIds = new Integer[bucketIdsSet.size()];
        bucketIdsSet.toArray(bucketIds);
        assertArrayEquals(expected, bucketIds);
    }

    protected void checkCounters(ObjectHistogram<BootstrappedVector> hist, double[] expected) {
        double[] counters = hist.buckets().stream().mapToDouble(x -> hist.getValue(x).get()).toArray();
        assertArrayEquals(expected, counters, 0.01);
    }

    protected BootstrappedVector randomVector(int countOfFeatures, int countOfSampes, boolean isClassification) {
        double[] features = DoubleStream.generate(() -> rnd.nextDouble()).limit(countOfFeatures).toArray();
        int[] counters = IntStream.generate(() -> rnd.nextInt(10)).limit(countOfSampes).toArray();
        double lbl = isClassification ? Math.abs(rnd.nextInt() % COUNT_OF_CLASSES) : rnd.nextDouble();
        return new BootstrappedVector(VectorUtils.of(features), lbl, counters);
    }

    protected <T extends Histogram<BootstrappedVector, T>> void checkSums(T expected, List<T> partitions) {
        T leftSum = partitions.stream().reduce((x,y) -> x.plus(y)).get();
        T rightSum = partitions.stream().reduce((x,y) -> y.plus(x)).get();
        assertTrue(expected.isEqualTo(leftSum));
        assertTrue(expected.isEqualTo(rightSum));
    }
}
