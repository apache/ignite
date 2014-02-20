// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

import org.gridgain.grid.dr.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Data center entry implementation containing only plain key and value.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrPlainEntry<K, V> implements GridDrEntry<K, V> {
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

    /**
     * Constructor.
     *
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     */
    public GridDrPlainEntry(K key, @Nullable V val, long ttl, long expireTime, GridCacheVersion ver) {
        assert ver != null;
        assert key != null;

        this.key = key;
        this.val = val;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
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
    @Override public String toString() {
        return S.toString(GridDrPlainEntry.class, this);
    }
}
