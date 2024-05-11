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

package org.apache.ignite.ml.tree;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Presenting decision tree data in plain manner (For example: from one parquet row filled with NodeData in Spark DT model).
 */
public class NodeData {
    /** Id. */
    public int id;

    /** Prediction. */
    public double prediction;

    /** Left child id. */
    public int leftChildId;

    /** Right child id. */
    public int rightChildId;

    /** Threshold. */
    public double threshold;

    /** Feature index. */
    public int featureIdx;

    /** Is leaf node. */
    public boolean isLeafNode;

    /**{@inheritDoc}*/
    @Override public String toString() {
        return "NodeData{" +
                "id=" + id +
                ", prediction=" + prediction +
                ", leftChildId=" + leftChildId +
                ", rightChildId=" + rightChildId +
                ", threshold=" + threshold +
                ", featureIdx=" + featureIdx +
                ", isLeafNode=" + isLeafNode +
                '}';
    }

    /**
     * Build tree or sub-tree based on indices and nodes sorted map as a dictionary.
     *
     * @param nodes The sorted map of nodes.
     * @param rootNodeData Root node data.
     */
    public static DecisionTreeNode buildTree(Map<Integer, NodeData> nodes,
                                             NodeData rootNodeData) {
        return rootNodeData.isLeafNode
            ? new DecisionTreeLeafNode(rootNodeData.prediction)
            : new DecisionTreeConditionalNode(
                rootNodeData.featureIdx,
                rootNodeData.threshold,
                buildTree(nodes, nodes.get(rootNodeData.rightChildId)),
                buildTree(nodes, nodes.get(rootNodeData.leftChildId)),
                null
            );
    }

    /**
     * Builds the DT model by the given sorted map of nodes.
     *
     * @param nodes The sorted map of nodes.
     */
    public static DecisionTreeModel buildDecisionTreeModel(Map<Integer, NodeData> nodes) {
        DecisionTreeModel mdl = null;
        if (!nodes.isEmpty()) {
            NodeData rootNodeData = (NodeData)((NavigableMap)nodes).firstEntry().getValue();
            mdl = new DecisionTreeModel(buildTree(nodes, rootNodeData));
            return mdl;
        }
        return mdl;
    }
}
