// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.query.*;

import javax.cache.event.*;

/**
 *
 */
public class CacheEntryEvent<K, V> extends javax.cache.event.CacheEntryEvent<K, V> {
    /** */
    private final GridCacheContinuousQueryEntry<K, V> e;

    /**
     * @param src Cache.
     * @param type Event type.
     * @param e Ignite event.
     */
    public CacheEntryEvent(IgniteCache src, EventType type, GridCacheContinuousQueryEntry<K, V> e) {
        super(src, type);

        this.e = e;
    }

    /** {@inheritDoc} */
    @Override public V getOldValue() {
        return e.getOldValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isOldValueAvailable() {
        return e.getOldValue() != null;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return e.getKey();
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return e.getValue();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> cls) {
        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheEntryEvent [evtType=" + getEventType() +
            ", key=" + getKey() +
            ", val=" + getValue() +
            ", oldVal=" + getOldValue() + ']';
    }
}
