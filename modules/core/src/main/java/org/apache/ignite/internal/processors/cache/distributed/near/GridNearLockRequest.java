package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;

public interface GridNearLockRequest extends Message, GridCacheDeployable, GridCacheVersionable {
    boolean firstClientRequest();

    AffinityTopologyVersion topologyVersion();

    UUID subjectId();

    int taskNameHash();

    boolean implicitTx();

    boolean implicitSingleTx();

    boolean syncCommit();

    CacheEntryPredicate[] filter();

    void filter(CacheEntryPredicate[] filter, GridCacheContext ctx)
        throws IgniteCheckedException;

    void hasTransforms(boolean hasTransforms);

    boolean hasTransforms();

    boolean needReturnValue();

    void addKeyBytes(
        KeyCacheObject key,
        boolean retVal,
        @Nullable GridCacheVersion dhtVer,
        GridCacheContext ctx
    ) throws IgniteCheckedException;

    GridCacheVersion dhtVersion(int idx);

    long accessTtl();

    boolean txRead();

    long timeout();

    long threadId();

    boolean skipStore();

    boolean keepBinary();

    boolean inTx();

    List<KeyCacheObject> keys();

    IgniteUuid futureId();

    TransactionIsolation isolation();

    boolean isInvalidate();

    int txSize();

    long messageId();

    boolean returnValue(int idx);

    int miniId();

    void miniId(int miniId);

    IgniteUuid oldVersionMiniId();
}
