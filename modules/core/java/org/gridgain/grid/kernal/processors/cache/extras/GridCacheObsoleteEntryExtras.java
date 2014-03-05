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

/**
 * Extras where obsolete version is set.
 */
public class GridCacheObsoleteEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /**
     * Constructor.
     *
     * @param obsoleteVer Obsolete version.
     */
    public GridCacheObsoleteEntryExtras(GridCacheVersion obsoleteVer) {
        assert obsoleteVer != null;

        this.obsoleteVer = obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> attributesData(GridLeanMap<String, Object> attrData) {
        return attrData != null ? new GridCacheAttributesObsoleteEntryExtras<K>(attrData, obsoleteVer) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(GridCacheMvcc<K> mvcc) {
        return mvcc != null ? new GridCacheMvccObsoleteEntryExtras<>(mvcc, obsoleteVer) : this;
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
            return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> ttlAndExpireTime(long ttl, long expireTime) {
        return ttl != 0 ? new GridCacheObsoleteTtlEntryExtras<K>(obsoleteVer, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheObsoleteEntryExtras.class, this);
    }
}
