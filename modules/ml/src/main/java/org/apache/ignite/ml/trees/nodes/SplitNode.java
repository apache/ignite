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

package org.apache.ignite.ml.trees.nodes;

import org.apache.ignite.ml.math.Vector;

/**
 * Node in decision tree representing a split.
 */
public abstract class SplitNode implements DecisionTreeNode {
    /** Left subtree. */
    protected DecisionTreeNode l;

    /** Right subtree. */
    protected DecisionTreeNode r;

    /** Feature index. */
    protected final int featureIdx;

    /**
     * Constructs SplitNode with a given feature index.
     *
     * @param featureIdx Feature index.
     */
    public SplitNode(int featureIdx) {
        this.featureIdx = featureIdx;
    }

    /**
     * Indicates if the given vector is in left subtree.
     *
     * @param v Vector
     * @return Status of given vector being left subtree.
     */
    abstract boolean goLeft(Vector v);

    /**
     * Left subtree.
     *
     * @return Left subtree.
     */
    public DecisionTreeNode left() {
        return l;
    }

    /**
     * Right subtree.
     *
     * @return Right subtree.
     */
    public DecisionTreeNode right() {
        return r;
    }

    /**
     * Set the left subtree.
     *
     * @param n left subtree.
     */
    public void setLeft(DecisionTreeNode n) {
        l = n;
    }

    /**
     * Set the right subtree.
     *
     * @param n right subtree.
     */
    public void setRight(DecisionTreeNode n) {
        r = n;
    }

    /**
     * Delegates processing to subtrees.
     *
     * @param v Vector.
     * @return Value assigned to the given vector.
     */
    @Override public double process(Vector v) {
        if (left() != null && goLeft(v))
            return left().process(v);
        else
            return right().process(v);
    }
}
