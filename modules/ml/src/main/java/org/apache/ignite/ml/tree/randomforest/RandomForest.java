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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetBuilder;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetPartition;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.histogram.BucketMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.ImpurityComputer;
import org.jetbrains.annotations.Nullable;

public abstract class RandomForest<S extends ImpurityComputer<BaggedVector, S>>
    extends DatasetTrainer<ModelsComposition, Double> {

    public static final double BUCKET_SIZE_FACTOR = (1 / 10.0);
    private final int countOfTrees;
    private final double subsampleSize;
    private final int maxDepth;
    private final double minImpurityDelta;
    private final List<FeatureMeta> meta;
    private final int featuresPerTree;
    private final Random random;

    public RandomForest(List<FeatureMeta> meta, int countOfTrees,
        double subsampleSize, int maxDepth, double minImpurityDelta,
        IgniteFunction<List<FeatureMeta>, Integer> featureCntSelectionStgy, long seed) {

        this.meta = meta;
        this.countOfTrees = countOfTrees;
        this.subsampleSize = subsampleSize;
        this.maxDepth = maxDepth;
        this.minImpurityDelta = minImpurityDelta;
        this.featuresPerTree = featureCntSelectionStgy.apply(meta);
        this.random = new Random(seed);
    }

    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        List<TreeRoot> models = null;
        try (Dataset<EmptyContext, BaggedDatasetPartition> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new BaggedDatasetBuilder<>(featureExtractor, lbExtractor, countOfTrees, subsampleSize))) {

            init(dataset);
            models = fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assert models != null;
        return buildComposition(models);
    }

    protected void init(Dataset<EmptyContext, BaggedDatasetPartition> dataset) {

    }

    private List<TreeRoot> fit(Dataset<EmptyContext, BaggedDatasetPartition> dataset) {
        Queue<TreeNode> treesQueue = createRootsQueue();
        ArrayList<TreeRoot> roots = initTrees(treesQueue);
        Map<Integer, BucketMeta> histMeta = computeHistogramMeta(meta, dataset);

        while (!treesQueue.isEmpty()) {
            //TODO: это не будет работать, так как поменяются ссылки на ноды - надо бы их восстанавливать
            Map<NodeId, TreeNode> nodesToLearn = getNodesToLearn(treesQueue);
            Map<NodeId, NodeWithStatistics> nodesStatistics = dataset.compute(
                x -> aggregateStatistics(x, roots, histMeta, nodesToLearn),
                this::reduce
            );

            for (IgniteBiTuple<Integer, Long> nodeKey : nodesStatistics.keySet()) {
                NodeWithStatistics statistics = nodesStatistics.get(nodeKey);
                NodeSplit bestSplit = statistics.findBestSplit();

                if (needSplit(statistics.node, bestSplit)) {
                    List<TreeNode> children = bestSplit.split(statistics.node);
                    treesQueue.addAll(children);
                }
                else {
                    bestSplit.createLeaf(statistics.node);
                }
            }
        }

        return roots;
    }

    protected ArrayList<TreeRoot> initTrees(Queue<TreeNode> treesQueue) {
        ArrayList<TreeRoot> roots = new ArrayList<>();

        for (TreeNode node : treesQueue) {
            Set<Integer> featuresSubspace = createFeaturesSubspace();
            roots.add(new TreeRoot(node, featuresSubspace));
        }

        return roots;
    }

    Set<Integer> createFeaturesSubspace() {
        assert featuresPerTree <= meta.size() : "Count of features per tree must be less or equal feature space size";

        Set<Integer> subspace = new HashSet<>();
        int count = 0;
        while (count < featuresPerTree) {
            int featureId = random.nextInt(meta.size());
            if (!subspace.contains(featureId)) {
                subspace.add(featureId);
                count++;
            }
        }

        return subspace;
    }

    static class NormalDistributionStats {
        private final double min;
        private final double max;
        private final double sumOfSquares;
        private final double sumOfValues;
        private final long n;

        public NormalDistributionStats(double min, double max, double sumOfSquares, double sumOfValues, long n) {
            this.min = min;
            this.max = max;
            this.sumOfSquares = sumOfSquares;
            this.sumOfValues = sumOfValues;
            this.n = n;
        }

        public NormalDistributionStats plus(NormalDistributionStats stats) {
            return new NormalDistributionStats(
                Math.min(this.min, stats.min),
                Math.max(this.max, stats.max),
                this.sumOfSquares + stats.sumOfSquares,
                this.sumOfValues + stats.sumOfValues,
                this.n + stats.n
            );
        }

        public double mean() {
            return sumOfValues / n;
        }

        public double variance() {
            double mean = mean();
            return (sumOfSquares / n) - mean * mean;
        }

        public double std() {
            return Math.sqrt(variance());
        }

        public double min() {
            return min;
        }

        public double max() {
            return max;
        }
    }

    private Map<Integer, BucketMeta> computeHistogramMeta(List<FeatureMeta> meta,
        Dataset<EmptyContext, BaggedDatasetPartition> dataset) {

        List<NormalDistributionStats> stats = dataset.compute(
            x -> computeStatsOnPartition(x, meta),
            (l, r) -> reduceStats(l, r, meta)
        );

        Map<Integer, BucketMeta> bucketsMeta = new HashMap<>();
        for (int i = 0; i < stats.size(); i++) {
            BucketMeta bucketMeta = new BucketMeta(meta.get(i));
            if (!bucketMeta.getFeatureMeta().isCategoricalFeature()) {
                NormalDistributionStats stat = stats.get(i);
                bucketMeta.setMinValue(stat.min());
                bucketMeta.setBucketSize(stat.std() * BUCKET_SIZE_FACTOR);
            }
        }
        return bucketsMeta;
    }

    List<NormalDistributionStats> computeStatsOnPartition(BaggedDatasetPartition part,
        List<FeatureMeta> meta) {

        double[] sumOfValues = new double[meta.size()];
        double[] sumOfSquares = new double[sumOfValues.length];
        double[] min = new double[sumOfValues.length];
        double[] max = new double[sumOfValues.length];
        Arrays.fill(min, Double.POSITIVE_INFINITY);
        Arrays.fill(max, Double.NEGATIVE_INFINITY);

        for (int i = 0; i < part.getRowsCount(); i++) {
            Vector vec = part.getRow(i).getFeatures();
            for (int featureId = 0; featureId < vec.size(); featureId++) {
                if (!meta.get(featureId).isCategoricalFeature()) {
                    double featureValue = vec.get(featureId);
                    sumOfValues[featureId] += featureValue;
                    sumOfSquares[featureId] += Math.pow(featureValue, 2);
                    min[featureId] = Math.min(min[featureId], featureValue);
                    max[featureId] = Math.max(max[featureId], featureValue);
                }
            }
        }

        ArrayList<NormalDistributionStats> res = new ArrayList<>();
        for (int featureId = 0; featureId < sumOfSquares.length; featureId++) {
            res.add(new NormalDistributionStats(
                min[featureId], max[featureId],
                sumOfSquares[featureId], sumOfValues[featureId],
                part.getRowsCount())
            );
        }
        return res;
    }

    List<NormalDistributionStats> reduceStats(List<NormalDistributionStats> left,
        List<NormalDistributionStats> right,
        List<FeatureMeta> meta) {

        assert meta.size() == left.size() && meta.size() == right.size();
        List<NormalDistributionStats> result = new ArrayList<>();
        for (int featureId = 0; featureId < meta.size(); featureId++) {
            NormalDistributionStats leftStat = left.get(featureId);
            NormalDistributionStats rightStat = right.get(featureId);
            result.add(leftStat.plus(rightStat));
        }
        return result;
    }

    private Queue<TreeNode> createRootsQueue() {
        Queue<TreeNode> roots = new LinkedList<>();
        for (int i = 0; i < countOfTrees; i++)
            roots.add(new TreeNode(1, i));
        return roots;
    }

    private Map<NodeId, TreeNode> getNodesToLearn(Queue<TreeNode> queue) {
        return queue.stream().collect(Collectors.toMap(
            node -> new NodeId(node.getTreeId(), node.getId()),
            node -> node
        ));
    }

    boolean needSplit(TreeNode parentNode, NodeSplit split) {
        return parentNode.getImpurity() - split.getImpurity() > minImpurityDelta &&
            parentNode.getDepth() < (maxDepth + 1);
    }

    protected abstract ModelsComposition buildComposition(List<TreeRoot> models);

    //TODO: need test
    Map<NodeId, NodeWithStatistics> aggregateStatistics(
        BaggedDatasetPartition dataset, ArrayList<TreeRoot> roots,
        Map<Integer, BucketMeta> histMeta,
        Map<NodeId, TreeNode> part) {

        Map<NodeId, NodeWithStatistics> res = new HashMap<>();
        dataset.foreach(vector -> {
            for (int sampleId = 0; sampleId < vector.getRepetitionsCounters().length; sampleId++) {
                TreeRoot root = roots.get(sampleId);
                long nodeId = root.node.predictNextNodeKey(vector.getFeatures());
                NodeId key = new NodeId(sampleId, nodeId);
                TreeNode cornerNode = part.get(key);
                if(cornerNode == null)
                    continue;

                if (!part.containsKey(key))
                    res.put(key, new NodeWithStatistics(cornerNode, new HashMap<>()));
                NodeWithStatistics statistics = res.get(key);

                for (int featureId = 0; featureId < vector.getFeatures().size(); featureId++) {
                    if (root.usedFeatures.contains(featureId)) {
                        BucketMeta meta = histMeta.get(featureId);
                        if (!statistics.perFeatureStatistics.containsKey(featureId))
                            statistics.perFeatureStatistics.put(featureId, createImpurityComputer(sampleId, meta));
                        S impurityComputer = statistics.perFeatureStatistics.get(featureId);
                        impurityComputer.addElement(vector);
                    }
                }
            }

        });
        return res;
    }

    protected abstract S createImpurityComputer(int sampleId, BucketMeta meta);

    //TODO: need test
    Map<NodeId, NodeWithStatistics> reduce(Map<NodeId, NodeWithStatistics> left,
        Map<NodeId, NodeWithStatistics> right) {

        Map<NodeId, NodeWithStatistics> result = new HashMap<>(left);
        for (NodeId key : right.keySet()) {
            NodeWithStatistics leftVal = left.get(key);
            if (!result.containsKey(key))
                result.put(key, leftVal);
            else
                result.get(key).addOtherStatistics(leftVal.perFeatureStatistics);
        }

        return result;
    }

    class NodeId extends IgniteBiTuple<Integer, Long> {
        public NodeId(@Nullable Integer treeId, @Nullable Long nodeId) {
            super(treeId, nodeId);
        }

        public Integer treeId() {
            return get1();
        }

        public Long nodeId() {
            return get2();
        }
    }

    class NodeWithStatistics {
        private final TreeNode node;
        private final Map<Integer, S> perFeatureStatistics; //not all features

        public NodeWithStatistics(TreeNode node, Map<Integer, S> perFeatureStatistics) {
            this.node = node;
            this.perFeatureStatistics = perFeatureStatistics;
        }

        public void addOtherStatistics(Map<Integer, S> other) {
            assert other.keySet().containsAll(perFeatureStatistics.keySet());

            for (Integer featureId : other.keySet())
                perFeatureStatistics.get(featureId).addHist(other.get(featureId));
        }

        public NodeSplit findBestSplit() {
            return perFeatureStatistics.values().stream().map(ImpurityComputer::findBestSplit)
                .min(Comparator.comparingDouble(NodeSplit::getImpurity))
                .get();
        }
    }

    public static class TreeRoot implements Model<Vector, Double> {
        private TreeNode node;
        private Set<Integer> usedFeatures;

        public TreeRoot(TreeNode root, Set<Integer> usedFeatures) {
            this.node = root;
            this.usedFeatures = usedFeatures;
        }

        @Override public Double apply(Vector vector) {
            return node.apply(vector);
        }

        public Set<Integer> getUsedFeatures() {
            return usedFeatures;
        }
    }
}
