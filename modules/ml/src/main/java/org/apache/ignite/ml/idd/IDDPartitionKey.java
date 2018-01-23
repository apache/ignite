package org.apache.ignite.ml.idd;

import java.util.UUID;

public class IDDPartitionKey {

    private final UUID iddId;

    private final int partId;

    public IDDPartitionKey(UUID iddId, int partId) {
        this.iddId = iddId;
        this.partId = partId;
    }

    public UUID getIddId() {
        return iddId;
    }

    public int getPartId() {
        return partId;
    }
}
