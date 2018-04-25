package org.apache.ignite.internal.processors.cache.verify;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;

/**
 */
public class ContentionInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ClusterNode node;

    /** */
    private List<String> entries;

    public ClusterNode getNode() {
        return node;
    }

    public void setNode(ClusterNode node) {
        this.node = node;
    }

    public List<String> getEntries() {
        return entries;
    }

    public void setEntries(List<String> entries) {
        this.entries = entries;
    }

    /** */
    public void print() {
        System.out.println("[node=" + node + ']');

        for (String entry : entries)
            System.out.println("    " + entry);
    }
}

