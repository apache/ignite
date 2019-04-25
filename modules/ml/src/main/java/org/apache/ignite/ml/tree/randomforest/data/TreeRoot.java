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

package org.apache.ignite.ml.tree.randomforest.data;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tree root class.
 */
public class TreeRoot implements IgniteModel<Vector, Double> {
    /** Serial version uid. */
    private static final long serialVersionUID = 531797299171329057L;

    /** Root node. */
    private TreeNode node;

    /** Used features. */
    private Set<Integer> usedFeatures;

    /**
     * Create an instance of TreeRoot.
     *
     * @param root Root.
     * @param usedFeatures Used features.
     */
    public TreeRoot(TreeNode root, Set<Integer> usedFeatures) {
        this.node = root;
        this.usedFeatures = usedFeatures;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector vector) {
        return node.predict(vector);
    }

    /** */
    public Set<Integer> getUsedFeatures() {
        return usedFeatures;
    }

    /** */
    public TreeNode getRootNode() {
        return node;
    }

    /**
     * @return All leafs in tree.
     */
    public List<TreeNode> getLeafs() {
        List<TreeNode> res = new ArrayList<>();
        getLeafs(node, res);
        return res;
    }

    /**
     * @param root Root.
     * @param res Result list.
     */
    private void getLeafs(TreeNode root, List<TreeNode> res) {
        if (root.getType() == TreeNode.Type.LEAF)
            res.add(root);
        else {
            getLeafs(root.getLeft(), res);
            getLeafs(root.getRight(), res);
        }
    }
}
