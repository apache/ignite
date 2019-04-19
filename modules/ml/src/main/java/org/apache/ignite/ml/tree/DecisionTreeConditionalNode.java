/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.tree;

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Decision tree conditional (non-leaf) node.
 */
public class DecisionTreeConditionalNode implements DecisionTreeNode {
    /** */
    private static final long serialVersionUID = 981630737007982172L;

    /** Column of the value to be tested. */
    private final int col;

    /** Threshold. */
    private final double threshold;

    /** Node that will be used in case tested value is greater then threshold. */
    private final DecisionTreeNode thenNode;

    /** Node that will be used in case tested value is not greater then threshold. */
    private final DecisionTreeNode elseNode;

    /**
     * Constructs a new instance of decision tree conditional node.
     *
     * @param col Column of the value to be tested.
     * @param threshold Threshold.
     * @param thenNode Node that will be used in case tested value is greater then threshold.
     * @param elseNode Node that will be used in case tested value is not greater then threshold.
     */
    DecisionTreeConditionalNode(int col, double threshold, DecisionTreeNode thenNode, DecisionTreeNode elseNode) {
        this.col = col;
        this.threshold = threshold;
        this.thenNode = thenNode;
        this.elseNode = elseNode;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector features) {
        return features.get(col) > threshold ? thenNode.apply(features) : elseNode.apply(features);
    }

    /** */
    public int getCol() {
        return col;
    }

    /** */
    public double getThreshold() {
        return threshold;
    }

    /** */
    public DecisionTreeNode getThenNode() {
        return thenNode;
    }

    /** */
    public DecisionTreeNode getElseNode() {
        return elseNode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return DecisionTree.printTree(this, pretty);
    }
}
