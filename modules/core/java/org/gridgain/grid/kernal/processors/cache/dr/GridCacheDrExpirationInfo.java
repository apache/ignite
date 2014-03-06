/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Cache DR info used as argument in PUT cache internal interfaces with expiration info added.
 */
public class GridCacheDrExpirationInfo<V> extends GridCacheDrInfo<V> {
    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     *
     */
    public GridCacheDrExpirationInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param val Value.
     * @param ver Version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheDrExpirationInfo(V val, GridCacheVersion ver, long ttl, long expireTime) {
        super(val, ver);

        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDrExpirationInfo.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeLong(ttl);
        out.writeLong(expireTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        ttl = in.readLong();
        expireTime = in.readLong();
    }
}
