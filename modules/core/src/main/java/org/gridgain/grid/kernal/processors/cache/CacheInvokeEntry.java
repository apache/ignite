/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;

/**
 * Implementation of {@link MutableEntry} passed to the {@link EntryProcessor#process(MutableEntry, Object...)}.
 */
public class CacheInvokeEntry<K, V> implements MutableEntry<K, V> {
    /** */
    @GridToStringInclude
    private final K key;

    /** */
    @GridToStringInclude
    private V val;

    /** */
    private boolean modified;

    /**
     * @param key Key.
     * @param val Value.
     */
    public CacheInvokeEntry(K key, @Nullable V val) {
        assert key != null;

        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public boolean exists() {
        return val != null;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        val = null;

        modified = true;
    }

    /** {@inheritDoc} */
    @Override public void setValue(V val) {
        if (val == null)
            throw new NullPointerException();

        this.val = val;

        modified = true;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        throw new IllegalArgumentException();
    }

    /**
     * @return {@code True} if {@link #setValue} or {@link #remove was called}.
     */
    public boolean modified() {
        return modified;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeEntry.class, this);
    }
}
