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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class TreeNode implements Model<Vector, Double>, Serializable {
    public enum Type {
        UNKNOWN, LEAF, CONDITIONAL
    }

    private final NodeId id;
    private int featureId;
    private double value;
    private Type type;
    private double impurity;
    private int depth;

    private TreeNode parent;
    private TreeNode left;
    private TreeNode right;

    public TreeNode(long id, int treeId) {
        this.id = new NodeId(treeId, id);
        this.value = -1.0;
        this.type = Type.UNKNOWN;
        this.impurity = Double.POSITIVE_INFINITY;
        this.depth = 1;
    }

    public Double apply(Vector features) {
        return predict(features);
    }

    private double predict(Vector features) {
        assert type != Type.UNKNOWN;

        if (type == Type.LEAF)
            return value;
        else {
            if (features.get(featureId) <= value)
                return left.predict(features);
            else
                return right.predict(features);
        }
    }

    public  NodeId predictNextNodeKey(Vector features) {
        switch (type) {
            case UNKNOWN:
                return id;
            case LEAF:
                return id;
            default:
                if (features.get(featureId) <= value)
                    return left.predictNextNodeKey(features);
                else
                    return right.predictNextNodeKey(features);
        }
    }

    public NodeId getId() {
        return id;
    }

    public List<TreeNode> toConditional(int featureId, double value) {
        assert type == Type.UNKNOWN;

        toLeaf(value);
        left = new TreeNode(2 * id.nodeId(), id.treeId());
        right = new TreeNode(2 * id.nodeId() + 1, id.treeId());
        this.type = Type.CONDITIONAL;
        this.featureId = featureId;

        left.parent = right.parent = this;
        left.depth = right.depth = depth + 1;

        return Arrays.asList(left, right);
    }

    public void toLeaf(double value) {
        assert type == Type.UNKNOWN;

        this.value = value;
        this.type = Type.LEAF;

        this.left = null;
        this.right = null;
    }

    public TreeNode getParent() {
        return parent;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public void setImpurity(double impurity) {
        this.impurity = impurity;
    }

    public double getImpurity() {
        return impurity;
    }

    public int getDepth() {
        return depth;
    }

    public TreeNode getLeft() {
        return left;
    }

    public TreeNode getRight() {
        return right;
    }
}
