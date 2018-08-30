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

package org.apache.ignite.ml.tree.randomforest.data.statistics;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class NormalDistributionStatisticsComputerTest {
    /** Features Meta. */
    private final List<FeatureMeta> meta = Arrays.asList(
        new FeatureMeta("", 0, false),
        new FeatureMeta("", 1, true),
        new FeatureMeta("", 2, false),
        new FeatureMeta("", 3, true),
        new FeatureMeta("", 4, false),
        new FeatureMeta("", 5, true),
        new FeatureMeta("", 6, false)
    );

    /** Partition. */
    private BootstrappedDatasetPartition partition = new BootstrappedDatasetPartition(new BootstrappedVector[] {
        new BootstrappedVector(VectorUtils.of(0, 1, 2, 1, 4, 2, 6), 0., null),
        new BootstrappedVector(VectorUtils.of(1, 0, 3, 2, 5, 3, 7), 0., null),
        new BootstrappedVector(VectorUtils.of(2, 1, 4, 1, 6, 2, 8), 0., null),
        new BootstrappedVector(VectorUtils.of(3, 0, 5, 2, 7, 3, 9), 0., null),
        new BootstrappedVector(VectorUtils.of(4, 1, 6, 1, 8, 2, 10), 0., null),
        new BootstrappedVector(VectorUtils.of(5, 0, 7, 2, 9, 3, 11), 0., null),
        new BootstrappedVector(VectorUtils.of(6, 1, 8, 1, 10, 2, 12), 0., null),
        new BootstrappedVector(VectorUtils.of(7, 0, 9, 2, 11, 3, 13), 0., null),
        new BootstrappedVector(VectorUtils.of(8, 1, 10, 1, 12, 2, 14), 0., null),
        new BootstrappedVector(VectorUtils.of(9, 0, 11, 2, 13, 3, 15), 0., null),
    });

    private NormalDistributionStatisticsComputer computer = new NormalDistributionStatisticsComputer();

    /** */
    @Test
    public void computeStatsOnPartitionTest() {
        List<NormalDistributionStatistics> result = computer.computeStatsOnPartition(partition, meta);
        NormalDistributionStatistics[] expected = new NormalDistributionStatistics[] {
            new NormalDistributionStatistics(0, 9, 285, 45, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(2, 11, 505, 65, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(4, 13, 805, 85, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(6, 15, 1185, 105, 10),
        };

        assertEquals(expected.length, result.size());
        for (int i = 0; i < expected.length; i++) {
            NormalDistributionStatistics expectedStat = expected[i];
            NormalDistributionStatistics resultStat = result.get(i);
            assertEquals(expectedStat.mean(), resultStat.mean(), 0.01);
            assertEquals(expectedStat.variance(), resultStat.variance(), 0.01);
            assertEquals(expectedStat.std(), resultStat.std(), 0.01);
            assertEquals(expectedStat.min(), resultStat.min(), 0.01);
            assertEquals(expectedStat.max(), resultStat.max(), 0.01);
        }
    }

    /** */
    @Test
    public void reduceStatsTest() {
        List<NormalDistributionStatistics> left = Arrays.asList(
            new NormalDistributionStatistics(0, 9, 285, 45, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(2, 11, 505, 65, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(4, 13, 805, 85, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(6, 15, 1185, 105, 10)
        );

        List<NormalDistributionStatistics> right = Arrays.asList(
            new NormalDistributionStatistics(6, 15, 1185, 105, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(4, 13, 805, 85, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(2, 11, 505, 65, 10),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(0, 9, 285, 45, 10)
        );

        List<NormalDistributionStatistics> result = computer.reduceStats(left, right, meta);
        NormalDistributionStatistics[] expected = new NormalDistributionStatistics[] {
            new NormalDistributionStatistics(0, 15, 1470, 150, 20),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(2, 13, 1310, 150, 20),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(2, 13, 1310, 150, 20),
            new NormalDistributionStatistics(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new NormalDistributionStatistics(0, 15, 1470, 150, 20)
        };

        assertEquals(expected.length, result.size());
        for (int i = 0; i < expected.length; i++) {
            NormalDistributionStatistics expectedStat = expected[i];
            NormalDistributionStatistics resultStat = result.get(i);
            assertEquals(expectedStat.mean(), resultStat.mean(), 0.01);
            assertEquals(expectedStat.variance(), resultStat.variance(), 0.01);
            assertEquals(expectedStat.std(), resultStat.std(), 0.01);
            assertEquals(expectedStat.min(), resultStat.min(), 0.01);
            assertEquals(expectedStat.max(), resultStat.max(), 0.01);
        }
    }
}
