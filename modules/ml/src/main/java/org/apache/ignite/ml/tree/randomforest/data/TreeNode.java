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
    enum Type {
        UNKNOWN, LEAF, CONDITIONAL
    }

    public static class Proba implements Serializable {
        private double value;
        private double probability;

        public Proba(double value, double probability) {
            this.value = value;
            this.probability = probability;
        }

        public double getValue() {
            return value;
        }

        public double getProbability() {
            return probability;
        }
    }

    private final int treeId;
    private final long id;
    private int featureId;
    private Proba value;
    private Type type;
    private double impurity;
    private int depth;

    private TreeNode parent;
    private TreeNode left;
    private TreeNode right;

    public TreeNode(long id, int treeId) {
        this.id = id;
        this.value = new Proba(Double.NaN, -1.0);
        this.type = Type.UNKNOWN;
        this.impurity = Double.NaN;
        this.depth = 1;
        this.treeId = treeId;
    }

    public Double apply(Vector features) {
        return predictProba(features).value;
    }

    public Proba predictProba(Vector features) {
        assert type != Type.UNKNOWN;

        if (type == Type.LEAF) {
            return value;
        }
        else {
            if (features.get(featureId) <= value.value)
                return left.predictProba(features);
            else
                return right.predictProba(features);
        }
    }

    long predictNextNodeKey(Vector features) {
        switch (type) {
            case UNKNOWN:
                return id;
            case LEAF:
                return -1;
            default:
                if (features.get(featureId) <= value.value)
                    return left.predictNextNodeKey(features);
                else
                    return right.predictNextNodeKey(features);
        }
    }

    public long getId() {
        return id;
    }

    public List<TreeNode> toConditional(int featureId, double value) {
        assert type == Type.UNKNOWN;

        toLeaf(value);
        left = new TreeNode(2 * id, treeId);
        right = new TreeNode(2 * id + 1, treeId);
        this.type = Type.CONDITIONAL;
        this.featureId = featureId;

        left.parent = right.parent = this;
        left.depth = right.depth = depth + 1;

        return Arrays.asList(left, right);
    }

    public void toLeaf(double value) {
        assert type == Type.UNKNOWN;

        this.value.value = value;
        this.type = Type.LEAF;

        this.left = null;
        this.right = null;
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

    public double getParentImpurity() {
        return parent == null ? Double.NaN : parent.impurity;
    }

    public int getTreeId() {
        return treeId;
    }

    public int getDepth() {
        return depth;
    }
}
