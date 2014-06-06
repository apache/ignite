/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.util;

import java.util.*;
import java.util.concurrent.*;

/**
 * Wrapper around concurrent map.
 */
public class GridConcurrentHashSet<E> extends AbstractSet<E> {
    /** Dummy value. */
    protected static final Object VAL = Boolean.TRUE;

    /** Base map. */
    protected ConcurrentMap<E, Object> map;

    /**
     * Creates new set based on {@link ConcurrentHashMap}.
     */
    public GridConcurrentHashSet() {
        this(new ConcurrentHashMap<E, Object>());
    }

    /**
     * Creates new set based on the given map.
     *
     * @param map Map to be used for set implementation.
     */
    @SuppressWarnings({"unchecked"})
    public GridConcurrentHashSet(ConcurrentMap<E, ?> map) {
        this.map = (ConcurrentMap<E, Object>)map;
    }

    /**
     * Gets wrapped map.
     *
     * @return Wrapped map.
     */
    @SuppressWarnings({"unchecked"})
    protected final <T extends Map<E, Object>> T  map() {
        return (T)map;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean add(E e) {
        return map.put(e, VAL) == null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return map.isEmpty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    @Override public boolean contains(Object o) {
        return map.containsKey(o);
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        return map.keySet().toArray();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousToArrayCall"})
    @Override public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map.clear();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return map.keySet().toString();
    }
}
