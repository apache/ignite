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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;
import org.apache.ignite.ml.tree.randomforest.data.NodeId;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.TreeRoot;
import org.apache.ignite.ml.tree.randomforest.data.impurity.ImpurityComputer;
import org.apache.ignite.ml.tree.randomforest.data.impurity.ImpurityHistogramsComputer;
import org.apache.ignite.ml.tree.randomforest.data.statistics.LeafValuesComputer;
import org.apache.ignite.ml.tree.randomforest.data.statistics.NormalDistributionStatistics;
import org.apache.ignite.ml.tree.randomforest.data.statistics.NormalDistributionStatisticsComputer;

/**
 * Class represents a realization of Random Forest algorithm. Main idea of this realization is that at each learning
 * iteration it tries to aggregate statistics on impurity for each corner nodes (leaves that may be splitted on a new
 * iteration) in each trees in random forest. It requires one map-reduce operation over learning dataset. After such
 * aggregation the algorithm selects split points for each corner nodes of create leaf nodes. The algorithm stops when
 * there is no splitting for nodes in trees. At last stage the algorithm aggregates statistics on labels for leaf nodes
 * in one map-reduce step and sets values to leafs based these statistics.
 *
 * @param <L> Type of statistics aggregator for leaf values computing.
 * @param <S> Type of impurity computer specific for algorithm.
 * @param <T> Type of child of RandomForestTrainer using in with-methods.
 */
public abstract class RandomForestTrainer<L, S extends ImpurityComputer<BootstrappedVector, S>,
    T extends RandomForestTrainer<L, S, T>> extends DatasetTrainer<ModelsComposition, Double> {
    /** Bucket size factor. */
    private static final double BUCKET_SIZE_FACTOR = (1 / 10.0);

    /** Count of trees. */
    private int amountOfTrees = 1;

    /** Subsample size. */
    private double subSampleSize = 1.0;

    /** Max depth. */
    private int maxDepth = 5;

    /** Min impurity delta. */
    private double minImpurityDelta = 0.0;

    /** Features Meta. */
    private List<FeatureMeta> meta;

    /** Features per tree. */
    private int featuresPerTree = 5;

    /** Seed. */
    private long seed = 1234L;

    /** Random generator. */
    private Random random = new Random(seed);

    /** Nodes to learn selection strategy. */
    private Function<Queue<TreeNode>, List<TreeNode>> nodesToLearnSelectionStrgy = this::defaultNodesToLearnSelectionStrgy;

    /**
     * Create an instance of RandomForestTrainer.
     *
     * @param meta Features Meta.
     */
    public RandomForestTrainer(List<FeatureMeta> meta) {
        this.meta = meta;
        this.featuresPerTree = FeaturesCountSelectionStrategies.ALL.apply(meta);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        List<TreeRoot> models = null;
        try (Dataset<EmptyContext, BootstrappedDatasetPartition> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new BootstrappedDatasetBuilder<>(featureExtractor, lbExtractor, amountOfTrees, subSampleSize))) {

            if(!init(dataset))
                return buildComposition(Collections.emptyList());
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
     * @param amountOfTrees Count of trees.
     * @return an instance of current object with valid type in according to inheritance.
     */
    public T withAmountOfTrees(int amountOfTrees) {
        this.amountOfTrees = amountOfTrees;
        return instance();
    }

    /**
     * @param subSampleSize Subsample size.
     * @return an instance of current object with valid type in according to inheritance.
     */
    public T withSubSampleSize(double subSampleSize) {
        this.subSampleSize = subSampleSize;
        return instance();
    }

    /**
     * @param maxDepth Max depth.
     * @return an instance of current object with valid type in according to inheritance.
     */
    public T withMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return instance();
    }

    /**
     * @param minImpurityDelta Min impurity delta.
     * @return an instance of current object with valid type in according to inheritance.
     */
    public T withMinImpurityDelta(double minImpurityDelta) {
        this.minImpurityDelta = minImpurityDelta;
        return instance();
    }

    /**
     * @param strgy Strgy.
     * @return an instance of current object with valid type in according to inheritance.
     */
    public T withFeaturesCountSelectionStrgy(Function<List<FeatureMeta>, Integer> strgy) {
        this.featuresPerTree = strgy.apply(meta);
        return instance();
    }

    /**
     * Sets strategy for selection nodes from learning queue in each iteration.
     *
     * @param strgy Strgy.
     */
    public T withNodesToLearnSelectionStrgy(Function<Queue<TreeNode>, List<TreeNode>> strgy) {
        this.nodesToLearnSelectionStrgy = strgy;
        return instance();
    }

    /**
     * @param seed Seed.
     * @return an instance of current object with valid type in according to inheritance.
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
    protected boolean init(Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {
        return true;
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
        if(histMeta.isEmpty())
            return Collections.emptyList();

        ImpurityHistogramsComputer<S> histogramsComputer = createImpurityHistogramsComputer();
        while (!treesQueue.isEmpty()) {
            Map<NodeId, TreeNode> nodesToLearn = getNodesToLearn(treesQueue);
            Map<NodeId, ImpurityHistogramsComputer.NodeImpurityHistograms<S>> nodesImpHists = histogramsComputer
                .aggregateImpurityStatistics(roots, histMeta, nodesToLearn, dataset);
            if (nodesToLearn.size() != nodesImpHists.size())
                throw new IllegalStateException();

            for (NodeId nodeId : nodesImpHists.keySet())
                split(treesQueue, nodesToLearn, nodesImpHists.get(nodeId));
        }

        createLeafStatisticsAggregator().setValuesForLeaves(roots, dataset);
        return roots;
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(ModelsComposition mdl) {
        ModelsComposition fakeComposition = buildComposition(Collections.emptyList());
        return mdl.getPredictionsAggregator().getClass() == fakeComposition.getPredictionsAggregator().getClass();
    }

    /** {@inheritDoc} */
    @Override protected <K, V> ModelsComposition updateModel(ModelsComposition mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        ArrayList<Model<Vector, Double>> oldModels = new ArrayList<>(mdl.getModels());
        ModelsComposition newModels = fit(datasetBuilder, featureExtractor, lbExtractor);
        oldModels.addAll(newModels.getModels());

        return new ModelsComposition(oldModels, mdl.getPredictionsAggregator());
    }

    /**
     * Split node with NodeId if need.
     *
     * @param learningQueue Learning queue.
     * @param nodesToLearn Nodes to learn at current iteration.
     * @param nodeImpurityHistograms Impurity histograms on current iteration.
     */
    private void split(Queue<TreeNode> learningQueue, Map<NodeId, TreeNode> nodesToLearn,
        ImpurityHistogramsComputer.NodeImpurityHistograms<S> nodeImpurityHistograms) {

        TreeNode cornerNode = nodesToLearn.get(nodeImpurityHistograms.getNodeId());
        Optional<NodeSplit> bestSplit = nodeImpurityHistograms.findBestSplit();

        if (needSplit(cornerNode, bestSplit)) {
            List<TreeNode> children = bestSplit.get().split(cornerNode);
            learningQueue.addAll(children);
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

    /**
     * Creates an instance of Histograms Computer corresponding to RF implementation.
     */
    protected abstract ImpurityHistogramsComputer<S> createImpurityHistogramsComputer();

    /**
     * Creates an instance of Leaf Statistics Aggregator corresponding to RF implementation.
     */
    protected abstract LeafValuesComputer<L> createLeafStatisticsAggregator();

    /**
     * Creates list of trees.
     *
     * @param treesQueue Trees queue.
     * @return List of trees.
     */
    protected ArrayList<TreeRoot> initTrees(Queue<TreeNode> treesQueue) {
        assert featuresPerTree > 0;
        ArrayList<TreeRoot> roots = new ArrayList<>();

        List<Integer> allFeatureIds = IntStream.range(0, meta.size()).boxed().collect(Collectors.toList());
        for (TreeNode node : treesQueue) {
            Collections.shuffle(allFeatureIds, random);
            Set<Integer> featuresSubspace = allFeatureIds.stream()
                .limit(featuresPerTree).collect(Collectors.toSet());
            roots.add(new TreeRoot(node, featuresSubspace));
        }

        return roots;
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

        List<NormalDistributionStatistics> stats = new NormalDistributionStatisticsComputer()
            .computeStatistics(meta, dataset);
        if(stats == null)
            return Collections.emptyMap();

        Map<Integer, BucketMeta> bucketsMeta = new HashMap<>();
        for (int i = 0; i < stats.size(); i++) {
            BucketMeta bucketMeta = new BucketMeta(meta.get(i));
            if (!bucketMeta.getFeatureMeta().isCategoricalFeature()) {
                NormalDistributionStatistics stat = stats.get(i);
                bucketMeta.setMinVal(stat.min());
                bucketMeta.setBucketSize(stat.std() * BUCKET_SIZE_FACTOR);
            }
            bucketsMeta.put(i, bucketMeta);
        }
        return bucketsMeta;
    }

    /**
     * Creates an initial nodes queue for learning based on countOfTrees. Each of these nodes represents a root decision
     * trees in random forest.
     *
     * @return initial nodes queue.
     */
    private Queue<TreeNode> createRootsQueue() {
        Queue<TreeNode> roots = new LinkedList<>();
        for (int i = 0; i < amountOfTrees; i++)
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
        List<TreeNode> res = new ArrayList<>(queue);
        queue.clear();
        return res;
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

}
