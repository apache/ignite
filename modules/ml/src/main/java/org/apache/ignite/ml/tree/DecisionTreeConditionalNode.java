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

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Decision tree conditional (non-leaf) node.
 */
public final class DecisionTreeConditionalNode implements DecisionTreeNode {
    /** */
    private static final long serialVersionUID = 981630737007982172L;

    /** Column of the value to be tested. */
    private final int col;

    /** Threshold. */
    private final double threshold;

    /** Node that will be used in case tested value is greater then threshold. */
    private DecisionTreeNode thenNode;

    /** Node that will be used in case tested value is not greater then threshold. */
    private DecisionTreeNode elseNode;

    /** Node that will be used in case tested value is not presented. */
    private DecisionTreeNode missingNode;

    /**
     * Constructs a new instance of decision tree conditional node.
     *
     * @param col Column of the value to be tested.
     * @param threshold Threshold.
     * @param thenNode Node that will be used in case tested value is greater then threshold.
     * @param elseNode Node that will be used in case tested value is not greater then threshold.
     * @param missingNode Node that will be used in case tested value is not presented.
     */
    public DecisionTreeConditionalNode(int col, double threshold, DecisionTreeNode thenNode, DecisionTreeNode elseNode,
        DecisionTreeNode missingNode) {
        this.col = col;
        this.threshold = threshold;
        this.thenNode = thenNode;
        this.elseNode = elseNode;
        this.missingNode = missingNode;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector features) {
        double val = features.get(col);

        if (Double.isNaN(val)) {
            if (missingNode == null)
                throw new IllegalArgumentException("Feature must not be null or missing node should be specified");

            return missingNode.predict(features);
        }

        return val > threshold ? thenNode.predict(features) : elseNode.predict(features);
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
    public void setThenNode(DecisionTreeNode thenNode) {
        this.thenNode = thenNode;
    }

    /** */
    public DecisionTreeNode getElseNode() {
        return elseNode;
    }

    /** */
    public void setElseNode(DecisionTreeNode elseNode) {
        this.elseNode = elseNode;
    }

    /** */
    public DecisionTreeNode getMissingNode() {
        return missingNode;
    }

    /** */
    public void setMissingNode(DecisionTreeNode missingNode) {
        this.missingNode = missingNode;
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
