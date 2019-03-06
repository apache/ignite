/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
