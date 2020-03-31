package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Partition destroy queue.
 */
public class PartitionDestroyQueue {
    /** */
    private final ConcurrentMap<T2<Integer, Integer>, PartitionDestroyRequest> pendingReqs =
        new ConcurrentHashMap<>();

    /**
     * @param grpCtx Group context.
     * @param partId Partition ID to destroy.
     */
    public void addDestroyRequest(@Nullable CacheGroupContext grpCtx, int grpId, int partId) {
        PartitionDestroyRequest req = new PartitionDestroyRequest(grpId, partId);

        PartitionDestroyRequest old = pendingReqs.putIfAbsent(new T2<>(grpId, partId), req);

        assert old == null || grpCtx == null : "Must wait for old destroy request to finish before adding a new one "
            + "[grpId=" + grpId
            + ", grpName=" + grpCtx.cacheOrGroupName()
            + ", partId=" + partId + ']';
    }

    /**
     * @param destroyId Destroy ID.
     * @return Destroy request to complete if was not concurrently cancelled.
     */
    private PartitionDestroyRequest beginDestroy(T2<Integer, Integer> destroyId) {
        PartitionDestroyRequest rmvd = pendingReqs.remove(destroyId);

        return rmvd == null ? null : rmvd.beginDestroy() ? rmvd : null;
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return Destroy request to wait for if destroy has begun.
     */
    public PartitionDestroyRequest cancelDestroy(int grpId, int partId) {
        PartitionDestroyRequest rmvd = pendingReqs.remove(new T2<>(grpId, partId));

        return rmvd == null ? null : !rmvd.cancel() ? rmvd : null;
    }

    /**
     * @return Pending reqs.
     */
    public ConcurrentMap<T2<Integer, Integer>, PartitionDestroyRequest> pendingReqs() {
        return pendingReqs;
    }
}
