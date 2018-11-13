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
 * Decision tree leaf node which contains value.
 */
public class DecisionTreeLeafNode implements DecisionTreeNode {
    /** */
    private static final long serialVersionUID = -472145568088482206L;

    /** Value of the node. */
    private final double val;

    /**
     * Constructs a new decision tree leaf node.
     *
     * @param val Value of the node.
     */
    public DecisionTreeLeafNode(double val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector doubles) {
        return val;
    }

    /** */
    public double getVal() {
        return val;
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
