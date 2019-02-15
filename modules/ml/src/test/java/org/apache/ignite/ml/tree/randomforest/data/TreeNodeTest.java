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

import java.util.List;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class TreeNodeTest {
    /** Features 1. */
    private final Vector features1 = VectorUtils.of(0., 1.);
    /** Features 2. */
    private final Vector features2 = VectorUtils.of(1., 0.);

    /** */
    @Test
    public void testPredictNextIdCondNodeAtTreeCorner() {
        TreeNode node = new TreeNode(5, 1);

        assertEquals(TreeNode.Type.UNKNOWN, node.getType());
        assertEquals(5, node.predictNextNodeKey(features1).nodeId());
        assertEquals(5, node.predictNextNodeKey(features2).nodeId());
    }

    /** */
    @Test
    public void testPredictNextIdForLeaf() {
        TreeNode node = new TreeNode(5, 1);
        node.toLeaf(0.5);

        assertEquals(TreeNode.Type.LEAF, node.getType());
        assertEquals(5, node.predictNextNodeKey(features1).nodeId());
        assertEquals(5, node.predictNextNodeKey(features2).nodeId());
    }

    /** */
    @Test
    public void testPredictNextIdForTree() {
        TreeNode root = new TreeNode(1, 1);
        root.toConditional(0, 0.1);

        assertEquals(TreeNode.Type.CONDITIONAL, root.getType());
        assertEquals(2, root.predictNextNodeKey(features1).nodeId());
        assertEquals(3, root.predictNextNodeKey(features2).nodeId());
    }

    /** */
    @Test
    public void testPredictProba() {
        TreeNode root = new TreeNode(1, 1);
        List<TreeNode> leaves = root.toConditional(0, 0.1);
        leaves.forEach(leaf -> {
            leaf.toLeaf(leaf.getId().nodeId() % 2);
        });

        assertEquals(TreeNode.Type.CONDITIONAL, root.getType());
        assertEquals(0.0, root.apply(features1), 0.001);
        assertEquals(1.0, root.apply(features2), 0.001);
    }
}
