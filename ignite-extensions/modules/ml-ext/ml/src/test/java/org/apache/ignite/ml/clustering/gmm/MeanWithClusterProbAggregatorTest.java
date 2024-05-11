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

package org.apache.ignite.ml.clustering.gmm;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MeanWithClusterProbAggregator}.
 */
public class MeanWithClusterProbAggregatorTest {
    /** */
    private MeanWithClusterProbAggregator aggregator1 = new MeanWithClusterProbAggregator();

    /** */
    private MeanWithClusterProbAggregator aggregator2 = new MeanWithClusterProbAggregator();

    /**
     * Default constructor.
     */
    public MeanWithClusterProbAggregatorTest() {
        aggregator1.add(VectorUtils.of(1., 1.), 0.5);
        aggregator1.add(VectorUtils.of(0., 1.), 0.25);
        aggregator1.add(VectorUtils.of(1., 0.), 0.75);
        aggregator1.add(VectorUtils.of(0., 0.), 0.10);

        aggregator2.add(VectorUtils.of(1., 1.), 1.0);
        aggregator2.add(VectorUtils.of(0., 1.), 1.0);
        aggregator2.add(VectorUtils.of(1., 0.), 1.0);
        aggregator2.add(VectorUtils.of(0., 0.), 1.0);
    }

    /** */
    @Test
    public void testAdd() {
        assertArrayEquals(new double[] {0.781, 0.468}, aggregator1.mean().asArray(), 1e-2);
        assertArrayEquals(new double[] {0.5, 0.5}, aggregator2.mean().asArray(), 1e-2);

        assertEquals(0.4, aggregator1.clusterProb(), 1e-4);
        assertEquals(1.0, aggregator2.clusterProb(), 1e-4);
    }

    /** */
    @Test
    public void testPlus() {
        MeanWithClusterProbAggregator res = aggregator1.plus(aggregator2);

        assertEquals(0.7, res.clusterProb(), 1e-4);
        assertArrayEquals(new double[] {0.580, 0.491}, res.mean().asArray(), 1e-2);
    }

    /** */
    @Test
    public void testReduce() {
        MeanWithClusterProbAggregator aggregator3 = new MeanWithClusterProbAggregator();
        MeanWithClusterProbAggregator aggregator4 = new MeanWithClusterProbAggregator();

        aggregator3.add(VectorUtils.of(1., 1.), 0.5);
        aggregator3.add(VectorUtils.of(0., 1.), 0.25);
        aggregator3.add(VectorUtils.of(1., 0.), 0.25);
        aggregator3.add(VectorUtils.of(0., 0.), 0.5);

        aggregator4.add(VectorUtils.of(1., 1.), 1.0);
        aggregator4.add(VectorUtils.of(0., 1.), 1.0);
        aggregator4.add(VectorUtils.of(1., 0.), 1.0);
        aggregator4.add(VectorUtils.of(0., 0.), 1.0);

        List<MeanWithClusterProbAggregator> res = MeanWithClusterProbAggregator.reduce(
            Arrays.asList(aggregator1, aggregator3),
            Arrays.asList(aggregator2, aggregator4)
        );

        MeanWithClusterProbAggregator res1 = res.get(0);
        assertEquals(0.70, res1.clusterProb(), 1e-2);
        assertArrayEquals(new double[] {0.580, 0.491}, res1.mean().asArray(), 1e-2);

        MeanWithClusterProbAggregator res2 = res.get(1);
        assertEquals(0.68, res2.clusterProb(), 1e-2);
        assertArrayEquals(new double[] {0.50, 0.50}, res2.mean().asArray(), 1e-2);
    }

    /** */
    @Test
    public void testMap() {
        GmmPartitionData data = new GmmPartitionData(
            Arrays.asList(
                new LabeledVector<>(VectorUtils.of(1, 0), 0.),
                new LabeledVector<>(VectorUtils.of(0, 1), 0.),
                new LabeledVector<>(VectorUtils.of(1, 1), 0.)
            ),

            new double[][] {
                new double[] {0.5, 0.1},
                new double[] {1.0, 0.4},
                new double[] {0.3, 0.2}
            }
        );

        List<MeanWithClusterProbAggregator> res = MeanWithClusterProbAggregator.map(data, 2);
        assertEquals(2, res.size());

        MeanWithClusterProbAggregator agg1 = res.get(0);
        assertEquals(0.6, agg1.clusterProb(), 1e-2);
        assertArrayEquals(new double[] {0.44, 0.72}, agg1.mean().asArray(), 1e-2);

        MeanWithClusterProbAggregator agg2 = res.get(1);
        assertEquals(0.23, agg2.clusterProb(), 1e-2);
        assertArrayEquals(new double[] {0.42, 0.85}, agg2.mean().asArray(), 1e-2);
    }
}
