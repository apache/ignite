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

/**
 * DGC bad lock.
 */
class GridCacheDgcBadLock implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near lock version. */
    private GridCacheVersion nearVer;

    /** DHT lock version. */
    private GridCacheVersion ver;

    /** Rollback flag. */
    private boolean rollback;

    /**
     * @param nearVer Near version.
     * @param ver DHT version.
     * @param rollback Rollback flag.
     */
    GridCacheDgcBadLock(@Nullable GridCacheVersion nearVer, GridCacheVersion ver, boolean rollback) {
        this.nearVer = nearVer;
        this.ver = ver;
        this.rollback = rollback;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheDgcBadLock() {
        // No-op.
    }

    /**
     * @return Near version.
     */
    @Nullable GridCacheVersion nearVersion() {
        return nearVer;
    }

    /**
     * @return Near version.
     */
    GridCacheVersion version() {
        return ver;
    }

    /**
     * @return {@code True} if near cache was checked.
     */
    boolean near() {
        return nearVer != null;
    }

    /**
     * @return Rollback flag.
     */
    boolean rollback() {
        return rollback;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        CU.writeVersion(out, nearVer);
        CU.writeVersion(out, ver);

        out.writeBoolean(rollback);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nearVer = CU.readVersion(in);
        ver = CU.readVersion(in);

        rollback = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcBadLock.class, this);
    }
}
