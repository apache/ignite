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
