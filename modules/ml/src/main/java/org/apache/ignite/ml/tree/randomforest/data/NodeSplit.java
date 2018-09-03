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

package org.apache.ignite.ml.tree.randomforest.data;

import java.util.List;

/**
 * Class represents a split point for decision tree.
 */
public class NodeSplit {
    /** Feature id in feature vector. */
    private final int featureId;

    /** Feature split value. */
    private final double value;

    /** Impurity at this split point. */
    private final double impurity;

    /**
     * Creates an instance of NodeSplit.
     *
     * @param featureId Feature id.
     * @param value Feature split value.
     * @param impurity Impurity value.
     */
    public NodeSplit(int featureId, double value, double impurity) {
        this.featureId = featureId;
        this.value = value;
        this.impurity = impurity;
    }

    /**
     * Split node from parameter onto two children nodes.
     *
     * @param node Node.
     * @return list of children.
     */
    public List<TreeNode> split(TreeNode node) {
        List<TreeNode> children = node.toConditional(featureId, value);
        node.setImpurity(impurity);
        return children;
    }

    /**
     * Convert node to leaf.
     *
     * @param node Node.
     */
    public void createLeaf(TreeNode node) {
        node.setImpurity(impurity);
        node.toLeaf(0.0); //values will be set in last stage if training
    }

    /** */
    public double getImpurity() {
        return impurity;
    }

    /** */
    public double getValue() {
        return value;
    }
}
