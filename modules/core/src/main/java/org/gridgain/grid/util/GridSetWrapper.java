/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.util.*;

/**
 * Set implementation that delegates to map.
 */
public class GridSetWrapper<E> extends GridSerializableSet<E> {
    /** Dummy value. */
    protected static final Object VAL = Boolean.TRUE;
    private static final long serialVersionUID = 0L;


    /** Base map. */
    @GridToStringExclude
    protected Map<E, Object> map;

    /**
     * Creates new set based on the given map.
     *
     * @param map Map to be used for set implementation.
     */
    @SuppressWarnings({"unchecked"})
    public GridSetWrapper(Map<E, ?> map) {
        A.notNull(map, "map");

        this.map = (Map<E, Object>)map;
    }

    /**
     * Creates new set based on the given map and initializes
     * it with given values.
     *
     * @param map Map to be used for set implementation.
     * @param initVals Initial values.
     */
    public GridSetWrapper(Map<E, ?> map, Collection<? extends E> initVals) {
        this(map);

        addAll(initVals);
    }

    /**
     * Provides default map value to child classes.
     *
     * @return Default map value.
     */
    protected final Object defaultValue() {
        return VAL;
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
        return S.toString(GridSetWrapper.class, this, "elements", map.keySet());
    }
}
