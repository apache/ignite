// @java.file.header

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
 * Extras where attributes, MVCC, obsolete version and TTL are set.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheAttributesMvccObsoleteTtlEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** Attributes data. */
    private GridLeanMap<String, Object> attrData;

    /** MVCC. */
    private GridCacheMvcc<K> mvcc;

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
     * @param mvcc MVCC.
     * @param obsoleteVer Obsolete version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheAttributesMvccObsoleteTtlEntryExtras(GridLeanMap<String, Object> attrData, GridCacheMvcc<K> mvcc,
        GridCacheVersion obsoleteVer, long ttl, long expireTime) {
        assert attrData != null;
        assert mvcc != null;
        assert obsoleteVer != null;
        assert ttl != 0;

        this.attrData = attrData;
        this.mvcc = mvcc;
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
            return new GridCacheMvccObsoleteTtlEntryExtras<>(mvcc, obsoleteVer, ttl ,expireTime);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvcc<K> mvcc() {
        return mvcc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(@Nullable GridCacheMvcc<K> mvcc) {
        if (mvcc != null) {
            this.mvcc = mvcc;

            return this;
        }
        else
            return new GridCacheAttributesObsoleteTtlEntryExtras<>(attrData, obsoleteVer, ttl, expireTime);
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
            return new GridCacheAttributesMvccTtlEntryExtras<>(attrData, mvcc, ttl, expireTime);
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
            return new GridCacheAttributesMvccObsoleteEntryExtras<>(attrData, mvcc, obsoleteVer);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesMvccObsoleteTtlEntryExtras.class, this);
    }
}
