/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * DGC lock candidate.
 */
class GridCacheDgcLockCandidate implements Externalizable {
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Near version. */
    private GridCacheVersion nearVer;

    /** DHT version. */
    private GridCacheVersion ver;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheDgcLockCandidate() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param nearVer Version.
     * @param ver DHT version.
     */
    GridCacheDgcLockCandidate(UUID nodeId, @Nullable GridCacheVersion nearVer, GridCacheVersion ver) {
        this.nodeId = nodeId;
        this.nearVer = nearVer;
        this.ver = ver;
    }

    /**
     * @return Node ID.
     */
    UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Near version.
     */
    @Nullable GridCacheVersion nearVersion() {
        return nearVer;
    }

    /**
     * @return DHT version.
     */
    GridCacheVersion version() {
        return ver;
    }

    /**
     * @return {@code True} if near cache should be checked.
     */
    boolean near() {
        return nearVer != null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        CU.writeVersion(out, nearVer);
        CU.writeVersion(out, ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        nearVer = CU.readVersion(in);
        ver = CU.readVersion(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcLockCandidate.class, this);
    }
}
