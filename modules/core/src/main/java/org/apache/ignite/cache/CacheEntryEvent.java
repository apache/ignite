// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache;

import org.apache.ignite.*;
import org.apache.ignite.events.*;

import javax.cache.event.*;

/**
 *
 */
public class CacheEntryEvent<K, V> extends javax.cache.event.CacheEntryEvent<K, V> {
    /** */
    private final IgniteCacheEvent evt;

    /**
     * @param src Cache.
     * @param type Event type.
     * @param evt Ignite event.
     */
    public CacheEntryEvent(IgniteCache src, EventType type, IgniteCacheEvent evt) {
        super(src, type);

        this.evt = evt;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V getOldValue() {
        return (V)evt.oldValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isOldValueAvailable() {
        return evt.hasOldValue();
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return evt.key();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V getValue() {
        return (V)evt.newValue();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls.equals(IgniteCacheEvent.class))
            return (T)evt;

        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheEntryEvent [evtType=" + getEventType() + ", evt=" + evt + ']';
    }
}
