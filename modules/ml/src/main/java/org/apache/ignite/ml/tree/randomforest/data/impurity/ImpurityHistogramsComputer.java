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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.randomforest.data.NodeId;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.TreeRoot;

/**
 * Class containing logic of aggregation impurity statistics within learning dataset.
 *
 * @param <S> Type of basic impurity computer for feature.
 */
public abstract class ImpurityHistogramsComputer<S extends ImpurityComputer<BootstrappedVector, S>> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -4984067145908187508L;

    /**
     * Computes histograms for each feature.
     *
     * @param roots Random forest roots.
     * @param histMeta Histograms meta.
     * @param nodesToLearn Nodes to learn.
     * @param dataset Dataset.
     */
    public Map<NodeId, NodeImpurityHistograms<S>> aggregateImpurityStatistics(ArrayList<TreeRoot> roots,
        Map<Integer, BucketMeta> histMeta, Map<NodeId, TreeNode> nodesToLearn,
        Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {

        return dataset.compute(
            x -> aggregateImpurityStatisticsOnPartition(x, roots, histMeta, nodesToLearn),
            this::reduceImpurityStatistics
        );
    }

    /**
     * Aggregates statistics for impurity computing for each corner nodes for each trees in random forest. This
     * algorithm predict corner node in decision tree for learning vector and stocks it to correspond histogram.
     *
     * @param dataset Dataset.
     * @param roots Trees.
     * @param histMeta Histogram buckets meta.
     * @param part Partition.
     * @return Leaf statistics for impurity computing.
     */
    private Map<NodeId, NodeImpurityHistograms<S>> aggregateImpurityStatisticsOnPartition(
        BootstrappedDatasetPartition dataset, ArrayList<TreeRoot> roots,
        Map<Integer, BucketMeta> histMeta,
        Map<NodeId, TreeNode> part) {

        Map<NodeId, NodeImpurityHistograms<S>> res = part.keySet().stream()
            .collect(Collectors.toMap(n -> n, NodeImpurityHistograms::new));

        dataset.forEach(vector -> {
            for (int sampleId = 0; sampleId < vector.counters().length; sampleId++) {
                if (vector.counters()[sampleId] == 0)
                    continue;

                TreeRoot root = roots.get(sampleId);
                NodeId key = root.getRootNode().predictNextNodeKey(vector.features());
                if (!part.containsKey(key)) //if we didn't take all nodes from learning queue
                    continue;

                NodeImpurityHistograms<S> statistics = res.get(key);
                for (Integer featureId : root.getUsedFeatures()) {
                    BucketMeta meta = histMeta.get(featureId);
                    if (!statistics.perFeatureStatistics.containsKey(featureId))
                        statistics.perFeatureStatistics.put(featureId, createImpurityComputerForFeature(sampleId, meta));
                    S impurityComputer = statistics.perFeatureStatistics.get(featureId);
                    impurityComputer.addElement(vector);
                }
            }
        });
        return res;
    }

    /**
     * Merge leaf statistics from several data partitions.
     *
     * @param left Left.
     * @param right Right.
     * @return Merged leaf impurity statistics.
     */
    private Map<NodeId, NodeImpurityHistograms<S>> reduceImpurityStatistics(Map<NodeId, NodeImpurityHistograms<S>> left,
        Map<NodeId, NodeImpurityHistograms<S>> right) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        Map<NodeId, NodeImpurityHistograms<S>> res = new HashMap<>(left);
        for (NodeId key : right.keySet()) {
            NodeImpurityHistograms<S> rightVal = right.get(key);
            if (!res.containsKey(key))
                res.put(key, rightVal);
            else
                res.put(key, left.get(key).plus(rightVal));
        }

        return res;
    }

    /**
     * Creates impurity computer in according to specific algorithm based on random forest (for example {@link
     * GiniHistogram} for classification).
     *
     * @param sampleId Sample id.
     * @param meta Bucket Meta.
     * @return Impurity computer
     */
    protected abstract S createImpurityComputerForFeature(int sampleId, BucketMeta meta);

    /**
     * Class represents per feature statistics for impurity computing.
     */
    public static class NodeImpurityHistograms<S extends ImpurityComputer<BootstrappedVector, S>> implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 2700045747590421768L;

        /** Node id. */
        private final NodeId nodeId;

        /** Per feature statistics. */
        private final Map<Integer, S> perFeatureStatistics = new HashMap<>();

        /**
         * Create an instance of NodeImpurityHistograms.
         *
         * @param nodeId Node id.
         */
        public NodeImpurityHistograms(NodeId nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * Store features statistics from other instance.
         *
         * @param other Other instance.
         */
        public NodeImpurityHistograms<S> plus(NodeImpurityHistograms<S> other) {
            assert nodeId.equals(other.nodeId);
            NodeImpurityHistograms<S> res = new NodeImpurityHistograms<>(nodeId);
            addTo(this.perFeatureStatistics, res.perFeatureStatistics);
            addTo(other.perFeatureStatistics, res.perFeatureStatistics);
            return res;
        }

        /**
         * Adds all statistics to target.
         *
         * @param from From.
         * @param to To.
         */
        private void addTo(Map<Integer, S> from, Map<Integer, S> to) {
            from.forEach((key, hist) -> {
                if (!to.containsKey(key))
                    to.put(key, hist);
                else {
                    S sumOfHists = to.get(key).plus(hist);
                    to.put(key, sumOfHists);
                }
            });
        }

        /** */
        public NodeId getNodeId() {
            return nodeId;
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
}
