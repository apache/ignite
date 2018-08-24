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
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;
import org.apache.ignite.ml.tree.randomforest.data.NodeId;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.impurity.GiniHistogram;
import org.apache.ignite.ml.tree.randomforest.data.impurity.ImpurityComputer;

/**
 * Class represents a realization of Random Forest algorithm. Main idea of this realization is that at each
 * learning iteration it try to aggregate statistics on impurity for each corner nodes in each trees in
 * random forest. It require one map-reduce operation over learning dataset. After such aggregation
 * the algorithm select split points for each corner nodes of create leaf nodes. The algorithm stops when
 * there is no splitting for nodes in trees. At last stage the algorithm aggregate statistics on labels for
 * leaf nodes in one map-reduce step and sets values to leafs based these statistics.
 *
 * @param <LeafStatsAggregator> Type of statistics aggregator for leaf values computing.
 * @param <S> Type of impurity computer specific for algorithm.
 * @param <T> Type of child of RandomForestTrainer using in with-methods.
 */
public abstract class RandomForestTrainer<LeafStatsAggregator, S extends ImpurityComputer<BootstrappedVector, S>, T extends RandomForestTrainer<LeafStatsAggregator, S, T>>
    extends DatasetTrainer<ModelsComposition, Double> {
    /** Bucket size factor. */
    public static final double BUCKET_SIZE_FACTOR = (1 / 10.0);

    /** Count of trees. */
    private int countOfTrees;

    /** Subsample size. */
    private double subsampleSize;

    /** Max depth. */
    private int maxDepth;

    /** Min impurity delta. */
    private double minImpurityDelta;

    /** Features Meta. */
    private List<FeatureMeta> meta;
    private int featuresPerTree;

    /** Random generator. */
    private Random random;

    /** Seed. */
    private long seed;

    /** Nodes to learn selection strategy. */
    private IgniteFunction<Queue<TreeNode>, List<TreeNode>> nodesToLearnSelectionStrgy;

    /**
     * Create an instance of RandomForestTrainer.
     *
     * @param meta Features Meta.
     */
    public RandomForestTrainer(List<FeatureMeta> meta) {
        this.meta = meta;
        this.countOfTrees = 1;
        this.subsampleSize = 1.0;
        this.maxDepth = 5;
        this.minImpurityDelta = 0.0;
        this.featuresPerTree = FeaturesCountSelectionStrategies.ALL.apply(meta);
        this.seed = System.currentTimeMillis();
        this.random = new Random(seed);
        this.nodesToLearnSelectionStrgy = this::defaultNodesToLearnSelectionStrgy;
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        List<TreeRoot> models = null;
        try (Dataset<EmptyContext, BootstrappedDatasetPartition> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new BootstrappedDatasetBuilder<>(featureExtractor, lbExtractor, countOfTrees, subsampleSize)
                .withSeed(seed))) {

            init(dataset);
            models = fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assert models != null;
        return buildComposition(models);
    }

    /**
     * @return an instance of current object with valid type in according to inheritance.
     */
    protected abstract T instance();

    /**
     * @param countOfTrees Count of trees.
     */
    public T withCountOfTrees(int countOfTrees) {
        this.countOfTrees = countOfTrees;
        return instance();
    }

    /**
     * @param subsampleSize Subsample size.
     */
    public T withSubsampleSize(double subsampleSize) {
        this.subsampleSize = subsampleSize;
        return instance();
    }

    /**
     * @param maxDepth Max depth.
     */
    public T withMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return instance();
    }

    /**
     * @param minImpurityDelta Min impurity delta.
     */
    public T withMinImpurityDelta(double minImpurityDelta) {
        this.minImpurityDelta = minImpurityDelta;
        return instance();
    }

    /**
     * @param strgy Strgy.
     */
    public T withFeaturesCountSelectionStrgy(IgniteFunction<List<FeatureMeta>, Integer> strgy) {
        this.featuresPerTree = strgy.apply(meta);
        return instance();
    }

    /**
     * Sets strategy for selection nodes from learning queue in each iteration.
     *
     * @param strgy Strgy.
     */
    public T withNodesToLearnSelectionStrgy(IgniteFunction<Queue<TreeNode>, List<TreeNode>> strgy) {
        this.nodesToLearnSelectionStrgy = strgy;
        return instance();
    }

    /**
     * @param seed Seed.
     */
    public T withSeed(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        return instance();
    }

    /**
     * Init-step before learning. It may be useful collecting labels statistics step for classification.
     *
     * @param dataset Dataset.
     */
    protected void init(Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {
    }

    /**
     * Trains model based on the specified data.
     *
     * @param dataset Dataset.
     * @return list of decision trees.
     */
    private List<TreeRoot> fit(Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {
        Queue<TreeNode> treesQueue = createRootsQueue();
        ArrayList<TreeRoot> roots = initTrees(treesQueue);
        Map<Integer, BucketMeta> histMeta = computeHistogramMeta(meta, dataset);

        while (!treesQueue.isEmpty()) {
            Map<NodeId, TreeNode> nodesToLearn = getNodesToLearn(treesQueue);
            Map<NodeId, NodeStatistics> nodesStatistics = dataset.compute(
                x -> aggregateImpurityStatistics(x, roots, histMeta, nodesToLearn),
                this::reduceImpurityStatistics
            );

            if (nodesToLearn.size() != nodesStatistics.size())
                throw new IllegalStateException();

            for (IgniteBiTuple<Integer, Long> nodeKey : nodesStatistics.keySet()) {
                NodeStatistics statistics = nodesStatistics.get(nodeKey);
                TreeNode cornerNode = nodesToLearn.get(statistics.nodeId);
                Optional<NodeSplit> bestSplit = statistics.findBestSplit();

                if (needSplit(cornerNode, bestSplit)) {
                    List<TreeNode> children = bestSplit.get().split(cornerNode);
                    treesQueue.addAll(children);
                }
                else {
                    if (bestSplit.isPresent())
                        bestSplit.get().createLeaf(cornerNode);
                    else {
                        cornerNode.setImpurity(Double.NEGATIVE_INFINITY);
                        cornerNode.toLeaf(0.0);
                    }
                }
            }
        }

        computeLeafValues(roots, dataset);
        return roots;
    }

    /**
     * Takes a list of all built trees and in one map-reduceImpurityStatistics step collect statistics for
     * evaluating leaf-values for each tree.
     *
     * @param roots Learned trees.
     * @param dataset Dataset.
     */
    protected void computeLeafValues(ArrayList<TreeRoot> roots,
        Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {

        Map<NodeId, TreeNode> leafs = roots.stream().flatMap(r -> r.getLeafs().stream())
            .collect(Collectors.toMap(TreeNode::getId, n -> n));

        Map<NodeId, LeafStatsAggregator> stats = dataset.compute(
            data -> computeLeafsStatisticsInPartition(roots, leafs, data),
            this::mergeLeafStatistics
        );

        leafs.forEach((id, leaf) -> {
            LeafStatsAggregator stat = stats.get(id);
            double leafVal = computeLeafValue(stat);
            leaf.setValue(leafVal);
        });
    }

    /**
     * Aggregates statistics on labels from learning dataset for each leaf nodes.
     *
     * @param roots Learned trees.
     * @param leafs List of all leafs.
     * @param data Data.
     * @return statistics on labels for each leaf nodes.
     */
    private Map<NodeId, LeafStatsAggregator> computeLeafsStatisticsInPartition(ArrayList<TreeRoot> roots,
        Map<NodeId, TreeNode> leafs, BootstrappedDatasetPartition data) {

        Map<NodeId, LeafStatsAggregator> res = new HashMap<>();
        for (int sampleId = 0; sampleId < roots.size(); sampleId++) {
            final int sampleIdConst = sampleId;

            data.foreach(vec -> {
                NodeId leafId = roots.get(sampleIdConst).getNode().predictNextNodeKey(vec.getFeatures());
                if (!leafs.containsKey(leafId))
                    throw new IllegalStateException();

                if (!res.containsKey(leafId))
                    res.put(leafId, createLeafStatsAggregator(sampleIdConst));

                addElementToLeafStatistic(res.get(leafId), vec, sampleIdConst);
            });
        }

        return res;
    }

    /**
     * Merges statistics on labels from several partitions.
     *
     * @param left first partition.
     * @param right second partition.
     * @return merged statistics.
     */
    private Map<NodeId, LeafStatsAggregator> mergeLeafStatistics(Map<NodeId, LeafStatsAggregator> left,
        Map<NodeId, LeafStatsAggregator> right) {
        if (left == null)
            return right;
        if (right == null)
            return left;

        Set<NodeId> keys = new HashSet<>(left.keySet());
        keys.addAll(right.keySet());
        for (NodeId key : keys) {
            if (!left.containsKey(key))
                left.put(key, right.get(key));
            else if (right.containsKey(key))
                left.put(key, mergeLeafStats(left.get(key), right.get(key)));
        }

        return left;
    }

    /**
     * Save vector to leaf statistic.
     *
     * @param leafStatAggr Leaf statistics aggregator.
     * @param vec Vector.
     * @param sampleId Sample id.
     */
    protected abstract void addElementToLeafStatistic(LeafStatsAggregator leafStatAggr, BootstrappedVector vec,
        int sampleId);

    /**
     * Merge statistics for same leafs.
     *
     * @param leafStatAggr1 First leaf stat aggregator.
     * @param leafStatAggr2 Second leaf stat aggregator.
     */
    protected abstract LeafStatsAggregator mergeLeafStats(
        LeafStatsAggregator leafStatAggr1, LeafStatsAggregator leafStatAggr2);

    /**
     * Creates an instance of leaf statistics aggregator in according to concrete algorithm based on RandomForest.
     *
     * @param sampleId Sample id.
     */
    protected abstract LeafStatsAggregator createLeafStatsAggregator(int sampleId);

    /**
     * Compute value from leaf based on statistics on labels corresponds to leaf.
     *
     * @param stat Leaf statistics.
     */
    protected abstract double computeLeafValue(LeafStatsAggregator stat);

    /**
     * Creates list of trees.
     *
     * @param treesQueue Trees queue.
     * @return List of trees.
     */
    protected ArrayList<TreeRoot> initTrees(Queue<TreeNode> treesQueue) {
        ArrayList<TreeRoot> roots = new ArrayList<>();

        for (TreeNode node : treesQueue) {
            Set<Integer> featuresSubspace = createFeaturesSubspace();
            roots.add(new TreeRoot(node, featuresSubspace));
        }

        return roots;
    }

    /**
     * Generate a set of feature ids for tree with size based on features count selection strategy.
     *
     * @return set of feature ids.
     */
    Set<Integer> createFeaturesSubspace() {
        if (featuresPerTree > meta.size())
            throw new RuntimeException("Count of features per tree must be less or equal feature space size");

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

    /**
     * Compute bucket metas based on feature metas and learning dataset.
     *
     * @param meta Features meta.
     * @param dataset Dataset.
     * @return bucket metas.
     */
    private Map<Integer, BucketMeta> computeHistogramMeta(List<FeatureMeta> meta,
        Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {

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
            bucketsMeta.put(i, bucketMeta);
        }
        return bucketsMeta;
    }

    /**
     * Aggregates normal distribution statistics for continual features in dataset partition.
     *
     * @param part Partition.
     * @param meta Meta.
     * @return Statistics for each feature.
     */
    List<NormalDistributionStats> computeStatsOnPartition(BootstrappedDatasetPartition part,
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

    /**
     * Merges statistics on features from two partitions.
     *
     * @param left Left.
     * @param right Right.
     * @param meta Features meta.
     * @return sum of statistics for each features.
     */
    List<NormalDistributionStats> reduceStats(List<NormalDistributionStats> left,
        List<NormalDistributionStats> right,
        List<FeatureMeta> meta) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        assert meta.size() == left.size() && meta.size() == right.size();
        List<NormalDistributionStats> result = new ArrayList<>();
        for (int featureId = 0; featureId < meta.size(); featureId++) {
            NormalDistributionStats leftStat = left.get(featureId);
            NormalDistributionStats rightStat = right.get(featureId);
            result.add(leftStat.plus(rightStat));
        }
        return result;
    }

    /**
     * Creates an initial nodes queue for learning based on countOfTrees.
     * Each of these nodes represents a root decision trees in random forest.
     *
     * @return initial nodes queue.
     */
    private Queue<TreeNode> createRootsQueue() {
        Queue<TreeNode> roots = new LinkedList<>();
        for (int i = 0; i < countOfTrees; i++)
            roots.add(new TreeNode(1, i));
        return roots;
    }

    /**
     * Select set of nodes for leaning from queue based in nodesToLearnSelectionStrategy.
     *
     * @param queue Learning queue.
     * @return collection of nodes for learning iterations.
     */
    private Map<NodeId, TreeNode> getNodesToLearn(Queue<TreeNode> queue) {
        return nodesToLearnSelectionStrgy.apply(queue).stream()
            .collect(Collectors.toMap(TreeNode::getId, node -> node));
    }

    /**
     * Default nodesToLearnSelectionStrategy that returns all nodes from queue.
     *
     * @param queue Queue.
     * @return List of nodes to learn.
     */
    private List<TreeNode> defaultNodesToLearnSelectionStrgy(Queue<TreeNode> queue) {
        List<TreeNode> result = new ArrayList<>(queue);
        queue.clear();
        return result;
    }

    /**
     * Check current note for the need for splitting.
     *
     * @param parentNode Parent node.
     * @param split Best node split.
     * @return true if split is needed.
     */
    boolean needSplit(TreeNode parentNode, Optional<NodeSplit> split) {
        return split.isPresent() && parentNode.getImpurity() - split.get().getImpurity() > minImpurityDelta &&
            parentNode.getDepth() < (maxDepth + 1);
    }

    /**
     * Returns composition of built trees.
     *
     * @param models Models.
     * @return composition of built trees.
     */
    protected abstract ModelsComposition buildComposition(List<TreeRoot> models);

    /**
     * Aggregates statistics for impurity computing for each corner nodes for each trees in random forest.
     * This algorithm predict corner node in decision tree for learning vector and stocks it to correspond histogram.
     *
     * @param dataset Dataset.
     * @param roots Trees.
     * @param histMeta Histogram buckets meta.
     * @param part Partition.
     * @return Leaf statistics for impurity computing.
     */
    Map<NodeId, NodeStatistics> aggregateImpurityStatistics(
        BootstrappedDatasetPartition dataset, ArrayList<TreeRoot> roots,
        Map<Integer, BucketMeta> histMeta,
        Map<NodeId, TreeNode> part) {

        Map<NodeId, NodeStatistics> res = part.keySet().stream()
            .collect(Collectors.toMap(n -> n, n -> new NodeStatistics(n, new HashMap<>())));

        dataset.foreach(vector -> {
            for (int sampleId = 0; sampleId < vector.getRepetitionsCounters().length; sampleId++) {
                if (vector.getRepetitionsCounters()[sampleId] == 0)
                    continue;

                TreeRoot root = roots.get(sampleId);
                NodeId key = root.node.predictNextNodeKey(vector.getFeatures());
                if (!part.containsKey(key)) //if we didn't take all nodes from learning queue
                    continue;

                NodeStatistics statistics = res.get(key);
                for (Integer featureId : root.getUsedFeatures()) {
                    BucketMeta meta = histMeta.get(featureId);
                    if (!statistics.perFeatureStatistics.containsKey(featureId))
                        statistics.perFeatureStatistics.put(featureId, createImpurityComputer(sampleId, meta));
                    S impurityComputer = statistics.perFeatureStatistics.get(featureId);
                    impurityComputer.addElement(vector);
                }
            }
        });
        return res;
    }

    /**
     * Creates impurity computer in according to specific algorithm based on random forest (for example
     * {@link GiniHistogram} for classification).
     *
     * @param sampleId Sample id.
     * @param meta Bucket Meta.
     * @return impurity computer
     */
    protected abstract S createImpurityComputer(int sampleId, BucketMeta meta);

    /**
     * Merge leaf statistics from several data partitions.
     *
     * @param left Left.
     * @param right Right.
     * @return merged leaf impurity statistics.
     */
    Map<NodeId, NodeStatistics> reduceImpurityStatistics(Map<NodeId, NodeStatistics> left,
        Map<NodeId, NodeStatistics> right) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        Map<NodeId, NodeStatistics> result = new HashMap<>(left);
        for (NodeId key : right.keySet()) {
            NodeStatistics rightVal = right.get(key);
            if (!result.containsKey(key))
                result.put(key, rightVal);
            else
                result.get(key).addOtherStatistics(rightVal.perFeatureStatistics);
        }

        return result;
    }

    /**
     * Aggregator of normal distribution statistics for continual features.
     */
    static class NormalDistributionStats {
        /** Min value. */
        private final double min;

        /** Max value. */
        private final double max;

        /** Sum of value squares. */
        private final double sumOfSquares;

        /** Sum of values. */
        private final double sumOfValues;

        /** Count of objects. */
        private final long n;

        /**
         * Creates an instance of NormalDistributionStats.
         *
         * @param min Min.
         * @param max Max.
         * @param sumOfSquares Sum of squares.
         * @param sumOfValues Sum of values.
         * @param n N.
         */
        public NormalDistributionStats(double min, double max, double sumOfSquares, double sumOfValues, long n) {
            this.min = min;
            this.max = max;
            this.sumOfSquares = sumOfSquares;
            this.sumOfValues = sumOfValues;
            this.n = n;
        }

        /**
         * Returns sum of normal distribution statistics.
         *
         * @param stats Stats.
         * @return sum of normal distribution statistics.
         */
        public NormalDistributionStats plus(NormalDistributionStats stats) {
            return new NormalDistributionStats(
                Math.min(this.min, stats.min),
                Math.max(this.max, stats.max),
                this.sumOfSquares + stats.sumOfSquares,
                this.sumOfValues + stats.sumOfValues,
                this.n + stats.n
            );
        }

        /**
         * Mean value.
         */
        public double mean() {
            return sumOfValues / n;
        }

        /**
         * Variance value.
         */
        public double variance() {
            double mean = mean();
            return (sumOfSquares / n) - mean * mean;
        }

        /**
         * Standard deviation value.
         */
        public double std() {
            return Math.sqrt(variance());
        }

        /**
         * Min value.
         */
        public double min() {
            return min;
        }

        /**
         * Max value.
         */
        public double max() {
            return max;
        }
    }

    /**
     * Class represents per feature statistics for impurity computing.
     */
    private class NodeStatistics {
        /** Node id. */
        private final NodeId nodeId;

        /** Per feature statistics. */
        private final Map<Integer, S> perFeatureStatistics;

        /**
         * Create an instance of NodeStatistics.
         *
         * @param nodeId Node id.
         * @param perFeatureStatistics Per feature statistics.
         */
        public NodeStatistics(NodeId nodeId, Map<Integer, S> perFeatureStatistics) {
            this.nodeId = nodeId;
            this.perFeatureStatistics = perFeatureStatistics;
        }

        /**
         * Store features statistics from other instance.
         *
         * @param other Other instance.
         */
        public void addOtherStatistics(Map<Integer, S> other) {
            assert other.keySet().containsAll(perFeatureStatistics.keySet());

            for (Integer featureId : other.keySet()) {
                if (!perFeatureStatistics.containsKey(featureId))
                    perFeatureStatistics.put(featureId, other.get(featureId));
                else
                    perFeatureStatistics.get(featureId).addHist(other.get(featureId));
            }
        }

        /**
         * Find best split point, based on feature statistics.
         *
         * @return Best split point if it exists.
         */
        public Optional<NodeSplit> findBestSplit() {
            return perFeatureStatistics.values().stream()
                .flatMap(x -> x.findBestSplit().map(Stream::of).orElse(Stream.empty()))
                .min(Comparator.comparingDouble(NodeSplit::getImpurity));
        }
    }

    /**
     * Tree root class.
     */
    protected static class TreeRoot implements Model<Vector, Double> {
        /** Root node. */
        private TreeNode node;

        /** Used features. */
        private Set<Integer> usedFeatures;

        /**
         * Create an instance of TreeRoot.
         *
         * @param root Root.
         * @param usedFeatures Used features.
         */
        public TreeRoot(TreeNode root, Set<Integer> usedFeatures) {
            this.node = root;
            this.usedFeatures = usedFeatures;
        }

        /** {@inheritDoc} */
        @Override public Double apply(Vector vector) {
            return node.apply(vector);
        }

        /** */
        public Set<Integer> getUsedFeatures() {
            return usedFeatures;
        }

        /** */
        public TreeNode getNode() {
            return node;
        }

        /**
         * @return all leafs in tree.
         */
        public List<TreeNode> getLeafs() {
            List<TreeNode> res = new ArrayList<>();
            getLeafs(node, res);
            return res;
        }

        /**
         * @param root Root.
         * @param res Result list.
         */
        private void getLeafs(TreeNode root, List<TreeNode> res) {
            if (root.getType() == TreeNode.Type.LEAF)
                res.add(root);
            else {
                getLeafs(root.getLeft(), res);
                getLeafs(root.getRight(), res);
            }
        }
    }
}
