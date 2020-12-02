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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Tree root class.
 */
public class RandomForestTreeModel implements IgniteModel<Vector, Double> {
    /** Serial version uid. */
    private static final long serialVersionUID = 531797299171329057L;

    /** Root node. */
    private TreeNode rootNode;

    /** Used features. */
    private Set<Integer> usedFeatures;

    /**
     * Create an instance of TreeRoot.
     *
     * @param root Root.
     * @param usedFeatures Used features.
     */
    public RandomForestTreeModel(TreeNode root, Set<Integer> usedFeatures) {
        this.rootNode = root;
        this.usedFeatures = usedFeatures;
    }

    public RandomForestTreeModel() {
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector vector) {
        return rootNode.predict(vector);
    }

    /** */
    public Set<Integer> getUsedFeatures() {
        return usedFeatures;
    }

    /** */
    public TreeNode getRootNode() {
        return rootNode;
    }

    /**
     * @return All leafs in tree.
     */
    public List<TreeNode> leafs() {
        List<TreeNode> res = new ArrayList<>();
        leafs(rootNode, res);
        return res;
    }

    /**
     * @param root Root.
     * @param res Result list.
     */
    private void leafs(TreeNode root, List<TreeNode> res) {
        if (root.getType() == TreeNode.Type.LEAF)
            res.add(root);
        else {
            leafs(root.getLeft(), res);
            leafs(root.getRight(), res);
        }
    }

    /**
     * Represents DecisionTree as String.
     *
     * @param node Decision tree.
     * @param pretty Use pretty mode.
     */
    public static String printTree(TreeNode node, boolean pretty) {
        StringBuilder builder = new StringBuilder();
        printTree(node, 0, builder, pretty, false);
        return builder.toString();
    }

    /**
     * Recursive implementation of DecisionTree to String converting.
     *
     * @param node Decision tree.
     * @param depth Current depth.
     * @param builder String builder.
     * @param pretty Use pretty mode.
     */
    private static void printTree(TreeNode node, int depth, StringBuilder builder, boolean pretty,
        boolean isThen) {
        builder.append(pretty ? String.join("", Collections.nCopies(depth, "\t")) : "");
        if (node.getType() == TreeNode.Type.LEAF) {
            TreeNode leaf = node;
            builder.append(String.format("%s return ", isThen ? "then" : "else"))
                .append(String.format("%.4f", leaf.getVal()));
        }
        else if (node.getType() == TreeNode.Type.CONDITIONAL) {
            TreeNode cond = node;
            String prefix = depth == 0 ? "" : (isThen ? "then " : "else ");
            builder.append(String.format("%sif (x", prefix))
                .append(cond.getFeatureId())
                .append(" > ")
                .append(String.format("%.4f", cond.getVal()))
                .append(pretty ? ")\n" : ") ");
            printTree(cond.getLeft(), depth + 1, builder, pretty, true);
            builder.append(pretty ? "\n" : " ");
            printTree(cond.getRight(), depth + 1, builder, pretty, false);
        }
        else
            throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return printTree(getRootNode(), pretty);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return printTree(getRootNode(), false);
    }
}
