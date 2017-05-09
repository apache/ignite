package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteCacheSnapshotManager extends GridCacheSharedManagerAdapter {
    /** Snapshot started lock filename. */
    public static final String SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME = "snapshot-started.loc";

    /**
     * @param initiatorNodeId Initiator node id.
     * @param snapshotOperation Snapshot operation.
     */
    @Nullable public IgniteInternalFuture startLocalSnapshotOperation(
        UUID initiatorNodeId,
        SnapshotOperation snapshotOperation
    ) throws IgniteCheckedException {
        return null;
    }

    /**
     * @param snapOp current snapshot operation.
     *
     * @return {@code true} if next operation must be snapshot, {@code false} if checkpoint must be executed.
     */
    public boolean onMarkCheckPointBegin(
        SnapshotOperation snapOp,
        NavigableMap<T2<Integer, Integer>, T2<Integer, Integer>> map
    ) throws IgniteCheckedException {
        return false;
    }

    /**
     *
     */
    public void restoreState() throws IgniteCheckedException {

    }

    /**
     *
     */
    public void onCheckPointBegin() {

    }

    /**
     *
     */
    public void beforeCheckpointPageWritten() {

    }

    /**
     *
     */
    public void afterCheckpointPageWritten() {

    }

    /**
     * @param fullId Full id.
     */
    public void checkPointCopyPage(FullPageId fullId) {

    }

    /**
     * @param fullId Full id.
     */
    public void checkPointBufferCopyPage(FullPageId fullId, ByteBuffer tmpWriteBuf) {

    }

    /**
     * @param cctx Cctx.
     */
    public void onCacheStop(GridCacheContext cctx) {

    }

    /**
     *
     */
    public GridInClosure3X<Long, FullPageId, PageMemory> changeTrackerPageHandler(){
        return new CIX3<Long, FullPageId, PageMemory>() {
            @Override public void applyx(Long aLong, FullPageId id, PageMemory memory) {
                // No-op.
            }
        };
    }

    /**
     *
     */
    public GridInClosure3X<FullPageId, ByteBuffer, Integer> flushDirtyPageHandler() {
        return new CIX3<FullPageId, ByteBuffer, Integer>() {
            @Override public void applyx(FullPageId fullId, ByteBuffer pageBuf, Integer tag) {
                // No-op.
            }
        };
    }
}
