/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.tree.randomforest.data;

import java.io.Serializable;
import java.util.List;

/**
 * Class represents a split point for decision tree.
 */
public class NodeSplit implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 1331311529596106124L;

    /** Feature id in feature vector. */
    private final int featureId;

    /** Feature split value. */
    private final double val;

    /** Impurity at this split point. */
    private final double impurity;

    /**
     * Creates an instance of NodeSplit.
     *
     * @param featureId Feature id.
     * @param val Feature split value.
     * @param impurity Impurity value.
     */
    public NodeSplit(int featureId, double val, double impurity) {
        this.featureId = featureId;
        this.val = val;
        this.impurity = impurity;
    }

    /**
     * Split node from parameter onto two children nodes.
     *
     * @param node Node.
     * @return list of children.
     */
    public List<TreeNode> split(TreeNode node) {
        List<TreeNode> children = node.toConditional(featureId, val);
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
    public double getVal() {
        return val;
    }
}
