/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.extras;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Extras where attributes, obsolete version and TTL are set.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheAttributesObsoleteTtlEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** Attributes data. */
    private GridLeanMap<String, Object> attrData;

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param attrData Attributes data.
     * @param obsoleteVer Obsolete version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheAttributesObsoleteTtlEntryExtras(GridLeanMap<String, Object> attrData, GridCacheVersion obsoleteVer,
        long ttl, long expireTime) {
        assert attrData != null;
        assert obsoleteVer != null;
        assert ttl != 0;

        this.attrData = attrData;
        this.obsoleteVer = obsoleteVer;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridLeanMap<String, Object> attributesData() {
        return attrData;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> attributesData(@Nullable GridLeanMap<String, Object> attrData) {
        if (attrData != null) {
            this.attrData = attrData;

            return this;
        }
        else
            return new GridCacheObsoleteTtlEntryExtras<>(obsoleteVer, ttl, expireTime);
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(GridCacheMvcc<K> mvcc) {
        return mvcc != null ? new GridCacheAttributesMvccObsoleteTtlEntryExtras<>(attrData, mvcc, obsoleteVer, ttl,
            expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion obsoleteVersion() {
        return obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> obsoleteVersion(GridCacheVersion obsoleteVer) {
        if (obsoleteVer != null) {
            this.obsoleteVer = obsoleteVer;

            return this;
        }
        else
            return new GridCacheAttributesTtlEntryExtras<>(attrData, ttl, expireTime);
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
    @Override public GridCacheEntryExtras<K> ttlAndExpireTime(long ttl, long expireTime) {
        if (ttl != 0) {
            this.ttl = ttl;
            this.expireTime = expireTime;

            return this;
        }
        else
            return new GridCacheAttributesObsoleteEntryExtras<>(attrData, obsoleteVer);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 32;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesObsoleteTtlEntryExtras.class, this);
    }
}
