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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.randomforest.data.NodeId;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.TreeRoot;

/**
 * Class containing logic of leaf values computing after building of all trees in random forest.
 *
 * @param <T> Type of leaf statistic.
 */
public abstract class LeafValuesComputer<T> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -429848953091775832L;

    /**
     * Takes a list of all built trees and in one map-reduceImpurityStatistics step collect statistics for evaluating
     * leaf-values for each tree and sets values for leaves.
     *
     * @param roots Learned trees.
     * @param dataset Dataset.
     */
    public void setValuesForLeaves(ArrayList<TreeRoot> roots,
        Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {

        Map<NodeId, TreeNode> leafs = roots.stream()
            .flatMap(r -> r.getLeafs().stream())
            .collect(Collectors.toMap(TreeNode::getId, Function.identity()));

        Map<NodeId, T> stats = dataset.compute(
            data -> computeLeafsStatisticsInPartition(roots, leafs, data),
            this::mergeLeafStatistics
        );

        leafs.forEach((id, leaf) -> {
            T stat = stats.get(id);
            if (stat != null) {
                double leafVal = computeLeafValue(stat);
                leaf.setVal(leafVal);
            }
        });
    }

    /**
     * Aggregates statistics on labels from learning dataset for each leaf nodes.
     *
     * @param roots Learned trees.
     * @param leafs List of all leafs.
     * @param data Data.
     * @return Statistics on labels for each leaf nodes.
     */
    private Map<NodeId, T> computeLeafsStatisticsInPartition(ArrayList<TreeRoot> roots,
        Map<NodeId, TreeNode> leafs, BootstrappedDatasetPartition data) {

        Map<NodeId, T> res = new HashMap<>();
        for (int sampleId = 0; sampleId < roots.size(); sampleId++) {
            final int sampleIdConst = sampleId;

            data.forEach(vec -> {
                NodeId leafId = roots.get(sampleIdConst).getRootNode().predictNextNodeKey(vec.features());
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
     * @return Merged statistics.
     */
    private Map<NodeId, T> mergeLeafStatistics(Map<NodeId, T> left, Map<NodeId, T> right) {
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
    protected abstract void addElementToLeafStatistic(T leafStatAggr, BootstrappedVector vec, int sampleId);

    /**
     * Merge statistics for same leafs.
     *
     * @param leftStats First leaf stat aggregator.
     * @param rightStats Second leaf stat aggregator.
     */
    protected abstract T mergeLeafStats(T leftStats, T rightStats);

    /**
     * Creates an instance of leaf statistics aggregator in according to concrete algorithm based on RandomForest.
     *
     * @param sampleId Sample id.
     */
    protected abstract T createLeafStatsAggregator(int sampleId);

    /**
     * Compute value from leaf based on statistics on labels corresponds to leaf.
     *
     * @param stat Leaf statistics.
     */
    protected abstract double computeLeafValue(T stat);
}
