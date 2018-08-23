package org.apache.ignite.ml.tree.randomforest.data;

import java.util.List;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TreeNodeTest {
    private final Vector features1 = VectorUtils.of(0., 1.);
    private final Vector features2 = VectorUtils.of(1., 0.);

    @Test
    public void testPredictNextIdCondNodeAtTreeCorner() {
        TreeNode node = new TreeNode(5, 1);

        assertEquals(TreeNode.Type.UNKNOWN, node.getType());
        assertEquals(5, node.predictNextNodeKey(features1));
        assertEquals(5, node.predictNextNodeKey(features2));
    }

    @Test
    public void testPredictNextIdForLeaf() {
        TreeNode node = new TreeNode(5, 1);
        node.toLeaf(0.5);

        assertEquals(TreeNode.Type.LEAF, node.getType());
        assertEquals(-1, node.predictNextNodeKey(features1));
        assertEquals(-1, node.predictNextNodeKey(features2));
    }

    @Test
    public void testPredictNextIdForTree() {
        TreeNode root = new TreeNode(1, 1);
        root.toConditional(0, 0.1);

        assertEquals(TreeNode.Type.CONDITIONAL, root.getType());
        assertEquals(2, root.predictNextNodeKey(features1));
        assertEquals(3, root.predictNextNodeKey(features2));
    }

    @Test
    public void testPredictProba() {
        TreeNode root = new TreeNode(1, 1);
        List<TreeNode> leaves = root.toConditional(0, 0.1);
        leaves.forEach(leaf -> {
            leaf.toLeaf(leaf.getId().nodeId() % 2);
        });

        assertEquals(TreeNode.Type.CONDITIONAL, root.getType());
        assertEquals(0.0, root.predictProba(features1).getValue(), 0.001);
        assertEquals(1.0, root.predictProba(features1).getProbability(), 0.001);

        assertEquals(1.0, root.predictProba(features2).getValue(), 0.001);
        assertEquals(1.0, root.predictProba(features2).getProbability(), 0.001);
    }
}
