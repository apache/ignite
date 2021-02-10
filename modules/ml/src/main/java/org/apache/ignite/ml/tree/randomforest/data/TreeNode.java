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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Decision tree node class.
 */
public class TreeNode implements IgniteModel<Vector, Double> {
    /** Serial version uid. */
    private static final long serialVersionUID = -8546263332508653661L;

    /**
     * Type of node.
     */
    public enum Type {
        /** Unknown. */
        UNKNOWN,

        /** Leaf node. */
        LEAF,

        /** Conditional node. */
        CONDITIONAL
    }

    /** Id. */
    private final NodeId id;

    /** Feature id. */
    private int featureId;

    /** Value. */
    private double val;

    /** Type. */
    private Type type;

    /** Impurity. */
    private double impurity;

    /** Depth. */
    private int depth;

    /** Left branch. */
    private TreeNode left;

    /** Right branch. */
    private TreeNode right;

    /**
     * Create an instance of TreeNode.
     *
     * @param id Id in according to breadth-first search ordering.
     * @param treeId Tree id.
     */
    public TreeNode(long id, int treeId) {
        this.id = new NodeId(treeId, id);
        this.val = -1;
        this.type = Type.UNKNOWN;
        this.impurity = Double.POSITIVE_INFINITY;
        this.depth = 1;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector features) {
        assert type != Type.UNKNOWN;

        if (type == Type.LEAF)
            return val;
        else {
            if (features.get(featureId) <= val)
                return left.predict(features);
            else
                return right.predict(features);
        }
    }

    /**
     * Returns leaf node for feature vector in according to decision tree.
     *
     * @param features Features.
     * @return Node.
     */
    public NodeId predictNextNodeKey(Vector features) {
        switch (type) {
            case UNKNOWN:
                return id;
            case LEAF:
                return id;
            default:
                if (features.get(featureId) <= val)
                    return left.predictNextNodeKey(features);
                else
                    return right.predictNextNodeKey(features);
        }
    }

    /**
     * Convert node to conditional node.
     *
     * @param featureId Feature id.
     * @param val Value.
     */
    public List<TreeNode> toConditional(int featureId, double val) {
        assert type == Type.UNKNOWN;

        toLeaf(val);
        left = new TreeNode(2 * id.nodeId(), id.treeId());
        right = new TreeNode(2 * id.nodeId() + 1, id.treeId());
        this.type = Type.CONDITIONAL;
        this.featureId = featureId;

        left.depth = right.depth = depth + 1;
        return Arrays.asList(left, right);
    }

    /**
     * Convert node to leaf.
     *
     * @param val Value.
     */
    public void toLeaf(double val) {
        assert type == Type.UNKNOWN;

        this.val = val;
        this.type = Type.LEAF;

        this.left = null;
        this.right = null;
    }

    /** */
    public NodeId getId() {
        return id;
    }

    /** */
    public void setVal(double val) {
        this.val = val;
    }

    /** */
    public Type getType() {
        return type;
    }

    /** */
    public double getVal() {
        return val;
    }

    /** */
    public void setImpurity(double impurity) {
        this.impurity = impurity;
    }

    /**
     * @return Impurity in current node.
     */
    public double getImpurity() {
        return impurity;
    }

    /**
     * @return Depth of current node.
     */
    public int getDepth() {
        return depth;
    }

    /**
     * @return Feature id.
     */
    public int getFeatureId() {
        return featureId;
    }

    /**
     * @return Right subtree.
     */
    public TreeNode getLeft() {
        return left;
    }

    /**
     * @return Left subtree.
     */
    public TreeNode getRight() {
        return right;
    }
}
