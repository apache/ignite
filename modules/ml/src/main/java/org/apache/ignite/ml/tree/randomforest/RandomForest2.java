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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetBuilder;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetPartition;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.BucketMeta;

public abstract class RandomForest2<S> extends DatasetTrainer<ModelsComposition, Double> {
    private final int countOfTrees;
    private final double subsampleSize;
    private final int maxDepth;
    private final double minImpurityDelta;
    private final FeatureMeta meta;

    public RandomForest2(FeatureMeta meta, int countOfTrees,
        double subsampleSize, int maxDepth, double minImpurityDelta) {

        this.meta = meta;
        this.countOfTrees = countOfTrees;
        this.subsampleSize = subsampleSize;
        this.maxDepth = maxDepth;
        this.minImpurityDelta = minImpurityDelta;
    }

    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        List<Model<Vector, Double>> models = null;
        try (Dataset<EmptyContext, BaggedDatasetPartition> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new BaggedDatasetBuilder<>(featureExtractor, lbExtractor, countOfTrees, subsampleSize))) {

            models = fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assert models != null;
        return buildComposition(models);
    }

    private List<Model<Vector, Double>> fit(Dataset<EmptyContext, BaggedDatasetPartition> dataset) {
        Queue<TreeNode> treesQueue = createRootsQueue();
        List<Model<Vector, Double>> roots = new ArrayList<>(treesQueue);
        Map<Integer, BucketMeta> histMeta = computeHistogramMeta(meta);

        while (!treesQueue.isEmpty()) {
            //node index = tree index + local node index
            Map<IgniteBiTuple<Integer, Long>, TreeNode> nodesToLearn = getNodesToLearn(treesQueue);
            Map<IgniteBiTuple<Integer, Long>, NodeWithStatistics> nodesStatistics = dataset.compute(
                x -> aggregateStatistics(histMeta, nodesToLearn),
                this::reduce
            );

            for (IgniteBiTuple<Integer, Long> nodeKey : nodesStatistics.keySet()) {
                NodeWithStatistics statistics = nodesStatistics.get(nodeKey);
                NodeSplit bestSplit = statistics.findBestSplit();

                if (needSplit(statistics, bestSplit)) {
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

    private Map<Integer, BucketMeta> computeHistogramMeta(FeatureMeta meta) {
        return null;
    }

    private Queue<TreeNode> createRootsQueue() {
        Queue<TreeNode> roots = new LinkedList<>();
        for (int i = 0; i < countOfTrees; i++)
            roots.add(new TreeNode(1, i));
        return roots;
    }

    private Map<IgniteBiTuple<Integer, Long>, TreeNode> getNodesToLearn(Queue<TreeNode> queue) {
        return queue.stream().collect(Collectors.toMap(
            node -> new IgniteBiTuple<>(node.getTreeId(), node.getId()),
            node -> node
        ));
    }

    boolean needSplit(NodeWithStatistics statistics, NodeSplit split) {
        return Math.abs(split.getImpurity() - statistics.node.getImpurity()) > minImpurityDelta &&
            statistics.node.getDepth() < maxDepth;
    }

    protected abstract ModelsComposition buildComposition(List<Model<Vector, Double>> models);

    protected abstract Map<IgniteBiTuple<Integer, Long>, NodeWithStatistics> aggregateStatistics(
        Map<Integer, BucketMeta> histMeta,
        Map<IgniteBiTuple<Integer, Long>, TreeNode> part);

    Map<IgniteBiTuple<Integer, Long>, NodeWithStatistics> reduce(
        Map<IgniteBiTuple<Integer, Long>, NodeWithStatistics> left,
        Map<IgniteBiTuple<Integer, Long>, NodeWithStatistics> right) {

        Map<IgniteBiTuple<Integer, Long>, NodeWithStatistics> result = new HashMap<>(left);
        for (IgniteBiTuple<Integer, Long> key : right.keySet()) {
            NodeWithStatistics leftValue = left.get(key);
            if (!result.containsKey(key))
                result.put(key, leftValue);
            else
                result.get(key).addOtherStatistics(leftValue.perFeatureStatistics);
        }

        return result;
    }

    abstract class NodeWithStatistics {
        private final TreeNode node;
        private final Map<Integer, S> perFeatureStatistics; //not all features

        public NodeWithStatistics(TreeNode node, Map<Integer, S> perFeatureStatistics) {
            this.node = node;
            this.perFeatureStatistics = perFeatureStatistics;
        }

        public abstract void addOtherStatistics(Map<Integer, S> other);

        public abstract NodeSplit findBestSplit();
    }
}
