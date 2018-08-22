package org.apache.ignite.ml.tree.randomforest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetPartition;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RandomForestTest {
    private final long seed = 0;
    private final int featuresPerTree = 4;
    private final int countOfTrees = 10;
    private final double minImpDelta = 1.0;
    private final int maxDepth = 1;

    private final List<FeatureMeta> meta = Arrays.asList(
        new FeatureMeta(0, false),
        new FeatureMeta(1, true),
        new FeatureMeta(2, false),
        new FeatureMeta(3, true),
        new FeatureMeta(4, false),
        new FeatureMeta(5, true),
        new FeatureMeta(6, false)
    );

    private RandomForestClassifier rf = new RandomForestClassifier(meta)
        .withCountOfTrees(countOfTrees)
        .withSeed(seed)
        .withFeaturesSelectionStrgy(x -> 4)
        .withMaxDepth(maxDepth)
        .withMinImpurityDelta(minImpDelta)
        .withSubsampleSize(0.1);

    @Test
    public void testCreateFeaturesSubspace() {
        Set<Integer> subspace = rf.createFeaturesSubspace();
        assertEquals(featuresPerTree, subspace.size());
    }

    @Test(expected = RuntimeException.class)
    public void testCreateFeaturesSubspaceFail() {
        new RandomForestClassifier(meta)
            .withCountOfTrees(countOfTrees)
            .withSeed(0L)
            .withFeaturesSelectionStrgy(x -> meta.size() + 1)
            .withMaxDepth(3)
            .withMinImpurityDelta(100.0)
            .withSubsampleSize(0.1)
            .createFeaturesSubspace();
    }

    private BaggedDatasetPartition partition = new BaggedDatasetPartition(new BaggedVector[] {
        new BaggedVector(VectorUtils.of(0, 1, 2, 1, 4, 2, 6), 0., null),
        new BaggedVector(VectorUtils.of(1, 0, 3, 2, 5, 3, 7), 0., null),
        new BaggedVector(VectorUtils.of(2, 1, 4, 1, 6, 2, 8), 0., null),
        new BaggedVector(VectorUtils.of(3, 0, 5, 2, 7, 3, 9), 0., null),
        new BaggedVector(VectorUtils.of(4, 1, 6, 1, 8, 2, 10), 0., null),
        new BaggedVector(VectorUtils.of(5, 0, 7, 2, 9, 3, 11), 0., null),
        new BaggedVector(VectorUtils.of(6, 1, 8, 1, 10, 2, 12), 0., null),
        new BaggedVector(VectorUtils.of(7, 0, 9, 2, 11, 3, 13), 0., null),
        new BaggedVector(VectorUtils.of(8, 1, 10, 1, 12, 2, 14), 0., null),
        new BaggedVector(VectorUtils.of(9, 0, 11, 2, 13, 3, 15), 0., null),
    });

    @Test
    public void testComputeStatsOnPartition() {
        List<RandomForest.NormalDistributionStats> result = rf.computeStatsOnPartition(partition, meta);
        RandomForest.NormalDistributionStats[] expected = new RandomForest.NormalDistributionStats[] {
            new RandomForest.NormalDistributionStats(0, 9, 285, 45, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(2, 11, 505, 65, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(4, 13, 805, 85, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(6, 15, 1185, 105, 10),
        };

        assertEquals(expected.length, result.size());
        for (int i = 0; i < expected.length; i++) {
            RandomForest.NormalDistributionStats expectedStat = expected[i];
            RandomForest.NormalDistributionStats resultStat = result.get(i);
            assertEquals(expectedStat.mean(), resultStat.mean(), 0.01);
            assertEquals(expectedStat.variance(), resultStat.variance(), 0.01);
            assertEquals(expectedStat.std(), resultStat.std(), 0.01);
            assertEquals(expectedStat.min(), resultStat.min(), 0.01);
            assertEquals(expectedStat.max(), resultStat.max(), 0.01);
        }
    }

    @Test
    public void testReduceStatistics() {
        List<RandomForest.NormalDistributionStats> left = Arrays.asList(
            new RandomForest.NormalDistributionStats(0, 9, 285, 45, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(2, 11, 505, 65, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(4, 13, 805, 85, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(6, 15, 1185, 105, 10)
        );

        List<RandomForest.NormalDistributionStats> right = Arrays.asList(
            new RandomForest.NormalDistributionStats(6, 15, 1185, 105, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(4, 13, 805, 85, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(2, 11, 505, 65, 10),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(0, 9, 285, 45, 10)
        );

        List<RandomForest.NormalDistributionStats> result = rf.reduceStats(left, right, meta);
        RandomForest.NormalDistributionStats[] expected = new RandomForest.NormalDistributionStats[] {
            new RandomForest.NormalDistributionStats(0, 15, 1470, 150, 20),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(2, 13, 1310, 150, 20),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(2, 13, 1310, 150, 20),
            new RandomForest.NormalDistributionStats(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0, 10),
            new RandomForest.NormalDistributionStats(0, 15, 1470, 150, 20)
        };

        assertEquals(expected.length, result.size());
        for (int i = 0; i < expected.length; i++) {
            RandomForest.NormalDistributionStats expectedStat = expected[i];
            RandomForest.NormalDistributionStats resultStat = result.get(i);
            assertEquals(expectedStat.mean(), resultStat.mean(), 0.01);
            assertEquals(expectedStat.variance(), resultStat.variance(), 0.01);
            assertEquals(expectedStat.std(), resultStat.std(), 0.01);
            assertEquals(expectedStat.min(), resultStat.min(), 0.01);
            assertEquals(expectedStat.max(), resultStat.max(), 0.01);
        }
    }

    @Test
    public void testNeedSplit() {
        TreeNode node = new TreeNode(1, 1);
        node.setImpurity(1000);
        assertTrue(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity() - minImpDelta * 1.01))));
        assertFalse(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity() - minImpDelta * 0.5))));
        assertFalse(rf.needSplit(node, Optional.of(new NodeSplit(0, 0, node.getImpurity()))));

        TreeNode child = node.toConditional(1000, 0, 0).get(0);
        assertFalse(rf.needSplit(child, Optional.of(new NodeSplit(0, 0, child.getImpurity() - minImpDelta * 1.01))));
    }
}
