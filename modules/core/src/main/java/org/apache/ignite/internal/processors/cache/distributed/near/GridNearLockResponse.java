package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

public interface GridNearLockResponse extends Message, GridCacheDeployable, GridCacheVersionable {
    @Nullable AffinityTopologyVersion clientRemapVersion();

    Collection<GridCacheVersion> pending();

    void pending(Collection<GridCacheVersion> pending);

    GridCacheVersion dhtVersion(int idx);

    GridCacheVersion mappedVersion(int idx);

    boolean filterResult(int idx);

    void addValueBytes(
        @Nullable CacheObject val,
        boolean filterPassed,
        @Nullable GridCacheVersion dhtVer,
        @Nullable GridCacheVersion mappedVer
    ) throws IgniteCheckedException;

    CacheObject value(int idx);

    Throwable error();

    Collection<GridCacheVersion> committedVersions();

    Collection<GridCacheVersion> rolledbackVersions();

    IgniteUuid futureId();

    void completedVersions(Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers);

    void error(Throwable err);
}
