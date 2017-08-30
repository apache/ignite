package org.apache.ignite.internal.processors.datastructures;

import java.util.UUID;

/** */
public final class NodeThread {
    /** */
    public final UUID nodeId;

    /** */
    public final long threadId;

    /** */
    public NodeThread(UUID nodeId, long threadId) {
        assert nodeId != null;

        this.nodeId = nodeId;
        this.threadId = threadId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NodeThread thread = (NodeThread)o;

        if (threadId != thread.threadId)
            return false;
        return nodeId.equals(thread.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = nodeId.hashCode();
        result = 31 * result + (int)(threadId ^ (threadId >>> 32));
        return result;
    }
}
