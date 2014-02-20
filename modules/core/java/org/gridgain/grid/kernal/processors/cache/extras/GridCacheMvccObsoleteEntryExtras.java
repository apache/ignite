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

/**
 * Extras where MVCC and obsolete version are set.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheMvccObsoleteEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** MVCC. */
    private GridCacheMvcc<K> mvcc;

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /**
     * Constructor.
     *
     * @param mvcc MVCC.
     * @param obsoleteVer Obsolete version.
     */
    public GridCacheMvccObsoleteEntryExtras(GridCacheMvcc<K> mvcc, GridCacheVersion obsoleteVer) {
        assert mvcc != null;
        assert obsoleteVer != null;

        this.mvcc = mvcc;
        this.obsoleteVer = obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> attributesData(GridLeanMap<String, Object> attrData) {
        return attrData != null ? new GridCacheAttributesMvccObsoleteEntryExtras<>(attrData, mvcc, obsoleteVer) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvcc<K> mvcc() {
        return mvcc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(GridCacheMvcc<K> mvcc) {
        if (mvcc != null) {
            this.mvcc = mvcc;

            return this;
        }
        else
            return new GridCacheObsoleteEntryExtras<>(obsoleteVer);
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
            return new GridCacheMvccEntryExtras<>(mvcc);
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> ttlAndExpireTime(long ttl, long expireTime) {
        return ttl != 0 ? new GridCacheMvccObsoleteTtlEntryExtras<>(mvcc, obsoleteVer, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMvccObsoleteEntryExtras.class, this);
    }
}
