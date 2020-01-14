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
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.ml.clustering.gmm.NewComponentStatisticsAggregator.computeNewMeanMap;
import static org.apache.ignite.ml.clustering.gmm.NewComponentStatisticsAggregator.computeNewMeanReduce;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NewComponentStatisticsAggregator} class.
 */
public class NewComponentStatisticsAggregatorTest {
    /** */
    GmmPartitionData data1 = new GmmPartitionData(
        Arrays.asList(
            vec(1, 0),
            vec(0, 1),
            vec(3, 7)
        ),
        new double[3][]
    );

    /** */
    GmmPartitionData data2 = new GmmPartitionData(
        Arrays.asList(
            vec(3, 1),
            vec(1, 4),
            vec(1, 3)
        ),
        new double[3][]
    );

    /** */
    GmmModel model;

    /** */
    @Before
    public void before() {
        model = mock(GmmModel.class);
        when(model.prob(data1.getX(0))).thenReturn(0.1);
        when(model.prob(data1.getX(1))).thenReturn(0.4);
        when(model.prob(data1.getX(2))).thenReturn(0.9);

        when(model.prob(data2.getX(0))).thenReturn(0.2);
        when(model.prob(data2.getX(1))).thenReturn(0.6);
        when(model.prob(data2.getX(2))).thenReturn(0.1);
    }

    /** */
    @Test
    public void testAdd() {
        NewComponentStatisticsAggregator agg = new NewComponentStatisticsAggregator();
        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            agg.add(VectorUtils.of(0, 1, 2), i % 2 == 0);

        assertEquals(rowCount / 2, agg.rowCountForNewCluster());
        assertEquals(rowCount, agg.totalRowCount());
        assertArrayEquals(new double[] {0, 1, 2}, agg.mean().asArray(), 1e-4);
    }

    /** */
    @Test
    public void testPlus() {
        NewComponentStatisticsAggregator agg1 = new NewComponentStatisticsAggregator();
        NewComponentStatisticsAggregator agg2 = new NewComponentStatisticsAggregator();
        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            agg1.add(VectorUtils.of(0, 1, 2), i % 2 == 0);

        for (int i = 0; i < rowCount; i++)
            agg2.add(VectorUtils.of(2, 1, 0), i % 2 == 1);

        NewComponentStatisticsAggregator sum = agg1.plus(agg2);
        assertEquals(rowCount, sum.rowCountForNewCluster());
        assertEquals(rowCount * 2, sum.totalRowCount());
        assertArrayEquals(new double[] {1, 1, 1}, sum.mean().asArray(), 1e-4);
    }

    /** */
    @Test
    public void testMap() {
        NewComponentStatisticsAggregator agg = computeNewMeanMap(data1, 1.0, 2, model);

        assertEquals(2, agg.rowCountForNewCluster());
        assertEquals(data1.size(), agg.totalRowCount());
        assertArrayEquals(new double[] {0.5, 0.5}, agg.mean().asArray(), 1e-4);
    }

    /** */
    @Test
    public void testReduce() {
        double maxXsProb = 1.0;
        int maxProbDivergence = 2;
        NewComponentStatisticsAggregator agg1 = computeNewMeanMap(data1, maxXsProb, maxProbDivergence, model);
        NewComponentStatisticsAggregator agg2 = computeNewMeanMap(data2, maxXsProb, maxProbDivergence, model);

        NewComponentStatisticsAggregator res = computeNewMeanReduce(agg1, null);
        assertEquals(agg1.rowCountForNewCluster(), res.rowCountForNewCluster());
        assertEquals(agg1.totalRowCount(), res.totalRowCount());
        assertArrayEquals(agg1.mean().asArray(), res.mean().asArray(), 1e-4);

        res = computeNewMeanReduce(null, agg1);
        assertEquals(agg1.rowCountForNewCluster(), res.rowCountForNewCluster());
        assertEquals(agg1.totalRowCount(), res.totalRowCount());
        assertArrayEquals(agg1.mean().asArray(), res.mean().asArray(), 1e-4);

        res = computeNewMeanReduce(agg2, agg1);
        assertEquals(4, res.rowCountForNewCluster());
        assertEquals(6, res.totalRowCount());
        assertArrayEquals(new double[] {1.25, 1.25}, res.mean().asArray(), 1e-4);

        res = computeNewMeanReduce(agg1, agg2);
        assertEquals(4, res.rowCountForNewCluster());
        assertEquals(6, res.totalRowCount());
        assertArrayEquals(new double[] {1.25, 1.25}, res.mean().asArray(), 1e-4);
    }

    /** */
    private LabeledVector<Double> vec(double... values) {
        return new LabeledVector<>(VectorUtils.of(values), 1.0);
    }
}
