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
        this.nodeId = nodeId;
        this.threadId = threadId;
    }
}
