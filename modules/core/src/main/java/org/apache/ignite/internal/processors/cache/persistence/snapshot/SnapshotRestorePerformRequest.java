package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

public class SnapshotRestorePerformRequest extends SnapshotRestoreRequest {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private final UUID updateMetaNodeId;

    public SnapshotRestorePerformRequest(String snpName, Collection<String> grps, Set<UUID> reqNodes, UUID updateMetaNodeId) {
        super(snpName, grps, reqNodes);

        this.updateMetaNodeId = updateMetaNodeId;
    }

    public UUID getUpdateMetaNodeId() {
        return updateMetaNodeId;
    }
}
