package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 *
 */
public class GridCachePlainVersionedEntry<K, V> implements GridCacheVersionedEntryEx<K, V> {
    /** Key. */
    private final K key;

    /** Value. */
    private final V val;

    /** TTL. */
    private final long ttl;

    /** Expire time. */
    private final long expireTime;

    /** Version. */
    private final GridCacheVersion ver;

    /** Start version flag. */
    private final boolean isStartVer;

    /**
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     */
    public GridCachePlainVersionedEntry(K key, @Nullable V val, long ttl, long expireTime, GridCacheVersion ver) {
        this(key, val, ttl, expireTime, ver, false);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param isStartVer Start version flag.
     */
    public GridCachePlainVersionedEntry(K key, V val, long ttl, long expireTime, GridCacheVersion ver,
        boolean isStartVer) {
        assert ver != null;
        assert key != null;

        this.key = key;
        this.val = val;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
        this.isStartVer = isStartVer;
    }

    /** {@inheritDoc} */
    @Override public K key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V value() {
        return val;
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
    @Override public byte dataCenterId() {
        return ver.dataCenterId();
    }

    /** {@inheritDoc} */
    @Override public int topologyVersion() {
        return ver.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return ver.order();
    }

    /** {@inheritDoc} */
    @Override public long globalTime() {
        return ver.globalTime();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean isStartVersion() {
        return isStartVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePlainVersionedEntry.class, this);
    }
}
