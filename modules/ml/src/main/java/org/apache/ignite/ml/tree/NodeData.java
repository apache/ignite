package org.apache.ignite.ml.tree;

/**
 * Presenting decision tree data in plain manner (For example: from one parquet row filled with NodeData in Spark DT model).
 */
public class NodeData {
    /** Id. */
    public int id;

    /** Prediction. */
    public double prediction;

    /** Left child id. */
    public int leftChildId;

    /** Right child id. */
    public int rightChildId;

    /** Threshold. */
    public double threshold;

    /** Feature index. */
    public int featureIdx;

    /** Is leaf node. */
    public boolean isLeafNode;

    /**{@inheritDoc}*/
    @Override
    public String toString() {
        return "NodeData{" +
                "id=" + id +
                ", prediction=" + prediction +
                ", leftChildId=" + leftChildId +
                ", rightChildId=" + rightChildId +
                ", threshold=" + threshold +
                ", featureIdx=" + featureIdx +
                ", isLeafNode=" + isLeafNode +
                '}';
    }

    /**
     * Build tree or sub-tree based on indices and nodes sorted map as a dictionary.
     *
     * @param nodes The sorted map of nodes.
     * @param rootNodeData Root node data.
     */
    @NotNull
    public static DecisionTreeNode buildTree(Map<Integer, NodeData> nodes,
                                              NodeData rootNodeData) {
        return rootNodeData.isLeafNode ? new DecisionTreeLeafNode(rootNodeData.prediction) : new DecisionTreeConditionalNode(rootNodeData.featureIdx,
                rootNodeData.threshold,
                buildTree(nodes, nodes.get(rootNodeData.rightChildId)),
                buildTree(nodes, nodes.get(rootNodeData.leftChildId)),
                null);
    }

    /**
     * Builds the DT model by the given sorted map of nodes.
     *
     * @param nodes The sorted map of nodes.
     */
    public static DecisionTreeModel buildDecisionTreeModel(Map<Integer, NodeData> nodes) {
        DecisionTreeModel mdl = null;
        if (!nodes.isEmpty()) {
            NodeData rootNodeData = (NodeData)((NavigableMap)nodes).firstEntry().getValue();
            mdl = new DecisionTreeModel(buildTree(nodes, rootNodeData));
            return mdl;
        }
        return mdl;
    }

}