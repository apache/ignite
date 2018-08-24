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

package org.apache.ignite.ml.tree.randomforest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class RandomForestTest {
    private final long seed = 0;
    private final int featuresPerTree = 4;
    private final int countOfTrees = 10;
    private final double minImpDelta = 1.0;
    private final int maxDepth = 1;

    /** Meta. */
    private final List<FeatureMeta> meta = Arrays.asList(
        new FeatureMeta(0, false),
        new FeatureMeta(1, true),
        new FeatureMeta(2, false),
        new FeatureMeta(3, true),
        new FeatureMeta(4, false),
        new FeatureMeta(5, true),
        new FeatureMeta(6, false)
    );

    /** Rf. */
    private RandomForestClassifierTrainer rf = new RandomForestClassifierTrainer(meta)
        .withCountOfTrees(countOfTrees)
        .withSeed(seed)
        .withFeaturesCountSelectionStrgy(x -> 4)
        .withMaxDepth(maxDepth)
        .withMinImpurityDelta(minImpDelta)
        .withSubsampleSize(0.1);

    /** */
    @Test
    public void testCreateFeaturesSubspace() {
        Set<Integer> subspace = rf.createFeaturesSubspace();
        assertEquals(featuresPerTree, subspace.size());
    }

    /** */
    @Test(expected = RuntimeException.class)
    public void testCreateFeaturesSubspaceFail() {
        new RandomForestClassifierTrainer(meta)
            .withCountOfTrees(countOfTrees)
            .withSeed(0L)
            .withFeaturesCountSelectionStrgy(x -> meta.size() + 1)
            .withMaxDepth(3)
            .withMinImpurityDelta(100.0)
            .withSubsampleSize(0.1)
            .createFeaturesSubspace();
    }

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

    /** */
    @Test
    public void testComputeStatsOnPartition() {
        List<RandomForestTrainer.NormalDistributionStats> result = rf.computeStatsOnPartition(partition, meta);
        RandomForestTrainer.NormalDistributionStats[] expected = new RandomForestTrainer.NormalDistributionStats[] {
            new RandomForestTrainer.NormalDistributionStats(0, 9, 285, 45, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(2, 11, 505, 65, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(4, 13, 805, 85, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(6, 15, 1185, 105, 10),
        };

        assertEquals(expected.length, result.size());
        for (int i = 0; i < expected.length; i++) {
            RandomForestTrainer.NormalDistributionStats expectedStat = expected[i];
            RandomForestTrainer.NormalDistributionStats resultStat = result.get(i);
            assertEquals(expectedStat.mean(), resultStat.mean(), 0.01);
            assertEquals(expectedStat.variance(), resultStat.variance(), 0.01);
            assertEquals(expectedStat.std(), resultStat.std(), 0.01);
            assertEquals(expectedStat.min(), resultStat.min(), 0.01);
            assertEquals(expectedStat.max(), resultStat.max(), 0.01);
        }
    }

    /** */
    @Test
    public void testReduceStatistics() {
        List<RandomForestTrainer.NormalDistributionStats> left = Arrays.asList(
            new RandomForestTrainer.NormalDistributionStats(0, 9, 285, 45, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(2, 11, 505, 65, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(4, 13, 805, 85, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(6, 15, 1185, 105, 10)
        );

        List<RandomForestTrainer.NormalDistributionStats> right = Arrays.asList(
            new RandomForestTrainer.NormalDistributionStats(6, 15, 1185, 105, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(4, 13, 805, 85, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(2, 11, 505, 65, 10),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(0, 9, 285, 45, 10)
        );

        List<RandomForestTrainer.NormalDistributionStats> result = rf.reduceStats(left, right, meta);
        RandomForestTrainer.NormalDistributionStats[] expected = new RandomForestTrainer.NormalDistributionStats[] {
            new RandomForestTrainer.NormalDistributionStats(0, 15, 1470, 150, 20),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(2, 13, 1310, 150, 20),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(2, 13, 1310, 150, 20),
            new RandomForestTrainer.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForestTrainer.NormalDistributionStats(0, 15, 1470, 150, 20)
        };

        assertEquals(expected.length, result.size());
        for (int i = 0; i < expected.length; i++) {
            RandomForestTrainer.NormalDistributionStats expectedStat = expected[i];
            RandomForestTrainer.NormalDistributionStats resultStat = result.get(i);
            assertEquals(expectedStat.mean(), resultStat.mean(), 0.01);
            assertEquals(expectedStat.variance(), resultStat.variance(), 0.01);
            assertEquals(expectedStat.std(), resultStat.std(), 0.01);
            assertEquals(expectedStat.min(), resultStat.min(), 0.01);
            assertEquals(expectedStat.max(), resultStat.max(), 0.01);
        }
    }

    /** */
    @Test
    public void testNeedSplit() {
        TreeNode node = new TreeNode(1, 1);
        node.setImpurity(1000);
        assertTrue(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity() - minImpDelta * 1.01))));
        assertFalse(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity() - minImpDelta * 0.5))));
        assertFalse(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity()))));

        TreeNode child = node.toConditional(0, 0).get(0);
        child.setImpurity(1000);
        assertFalse(rf.needSplit(child, Optional.of(new NodeSplit(0, 0, child.getImpurity() - minImpDelta * 1.01))));
    }
}
