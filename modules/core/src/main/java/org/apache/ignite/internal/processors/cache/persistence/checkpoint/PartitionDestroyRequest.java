package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Partition destroy request.
 */
public class PartitionDestroyRequest {
    /** */
    private final int grpId;

    /** */
    private final int partId;

    /** Destroy cancelled flag. */
    private boolean cancelled;

    /** Destroy future. Not null if partition destroy has begun. */
    private GridFutureAdapter<Void> destroyFut;

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    PartitionDestroyRequest(int grpId, int partId) {
        this.grpId = grpId;
        this.partId = partId;
    }

    /**
     * Cancels partition destroy request.
     *
     * @return {@code False} if this request needs to be waited for.
     */
    public synchronized boolean cancel() {
        if (destroyFut != null) {
            assert !cancelled;

            return false;
        }

        cancelled = true;

        return true;
    }

    /**
     * Initiates partition destroy.
     *
     * @return {@code True} if destroy request should be executed, {@code false} otherwise.
     */
    public synchronized boolean beginDestroy() {
        if (cancelled) {
            assert destroyFut == null;

            return false;
        }

        if (destroyFut != null)
            return false;

        destroyFut = new GridFutureAdapter<>();

        return true;
    }

    /**
     *
     */
    public synchronized void onDone(Throwable err) {
        assert destroyFut != null;

        destroyFut.onDone(err);
    }

    /**
     *
     */
    public void waitCompleted() throws IgniteCheckedException {
        GridFutureAdapter<Void> fut;

        synchronized (this) {
            assert destroyFut != null;

            fut = destroyFut;
        }

        fut.get();
    }

    /**
     * @return Group id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Partition id.
     */
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "PartitionDestroyRequest [grpId=" + grpId + ", partId=" + partId + ']';
    }
}
