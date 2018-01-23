package org.apache.ignite.ml.idd;

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

public class IDDPartitionDistributedSegment<K, V, D, L> {

    private final IgniteBiFunction<IgniteCache<K, V>, Integer, L> loader;

    private final UUID iddId;

    private final int partId;

    private final D distributedSegment;

    public IDDPartitionDistributedSegment(
        IgniteBiFunction<IgniteCache<K, V>, Integer, L> loader, UUID iddId, int partId, D distributedSegment) {
        this.loader = loader;
        this.iddId = iddId;
        this.partId = partId;
        this.distributedSegment = distributedSegment;
    }

    public IgniteBiFunction<IgniteCache<K, V>, Integer, L> getLoader() {
        return loader;
    }

    public UUID getIddId() {
        return iddId;
    }

    public int getPartId() {
        return partId;
    }

    public D getDistributedSegment() {
        return distributedSegment;
    }
}
