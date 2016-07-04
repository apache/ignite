package org.apache.ignite.internal.processors.hadoop.planner;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.ArrayList;

/**
 * Map-reduce plan group.
 */
public class HadoopMapReducePlanGroup {
    /** Node. */
    private ClusterNode node;

    /** Nodes. */
    private ArrayList<ClusterNode> nodes;

    /** CPUs. */
    private final int cpus;

    /**
     * Constructor.
     *
     * @param node First node in the group.
     */
    public HadoopMapReducePlanGroup(ClusterNode node) {
        this.node = node;

        cpus = node.metrics().getTotalCpus();
    }

    /**
     * Add node to the group.
     *
     * @param newNode New node.
     */
    public void add(ClusterNode newNode) {
        assert newNode.metrics().getTotalCpus() == cpus;

        if (node != null) {
            nodes = new ArrayList<>(2);

            nodes.add(node);

            node = null;
        }

        nodes.add(newNode);
    }

    /**
     * @return {@code True} if only sinle node present.
     */
    public boolean single() {
        return nodeCount() == 1;
    }

    /**
     * Get node by index.
     *
     * @param idx Index.
     * @return Node.
     */
    public ClusterNode node(int idx) {
        if (node != null) {
            assert idx == 0;

            return node;
        }
        else {
            assert nodes != null;
            assert idx < nodes.size();

            return nodes.get(idx);
        }
    }

    /**
     * @return Node count.
     */
    public int nodeCount() {
        return node != null ? 1 : nodes.size();
    }

    /**
     * @return CPU count.
     */
    public int cpuCount() {
        return cpus;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopMapReducePlanGroup.class, this);
    }
}
