package org.apache.ignite.internal.visor.tx;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;

/**
 */
public class VisorTxNodeInfo implements Serializable, Comparable<VisorTxNodeInfo> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID id;

    /** */
    private final Object consistentId;

    /** */
    private final long order;

    /**
     * @param node Cluster node.
     */
    public VisorTxNodeInfo(ClusterNode node) {
        this.id = node.id();
        this.consistentId = node.consistentId();
        this.order = node.order();
    }

    /** */
    public UUID getId() {
        return id;
    }

    /** */
    public Object getConsistentId() {
        return consistentId;
    }

    /** */
    public long getOrder() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        VisorTxNodeInfo info = (VisorTxNodeInfo)o;

        return consistentId.equals(info.consistentId);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return consistentId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(VisorTxNodeInfo info) {
        return Long.compare(order, info.order);
    }
}
