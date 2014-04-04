/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import java.util.*;

/**
 * Set implementation.
 */
public class GridCacheSet<T> implements Set<T> {
    /** */
    private final GridCacheContext ctx;

    /** */
    private final GridCache<GridCacheSetItemKey, Boolean> cache;

    /** */
    private final String name;

    /** */
    private final GridUuid id;

    /**
     * @param ctx Cache context.
     * @param name Set name.
     * @param id Set unique ID.
     */
    @SuppressWarnings("unchecked")
    public GridCacheSet(GridCacheContext ctx, String name, GridUuid id) {
        this.ctx = ctx;
        this.name = name;
        this.id = id;

        cache = ctx.cache();
    }

    /**
     * @return Set ID.
     */
    public GridUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        try {
            return cache.get(itemKey(o)) != null;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(T t) {
        try {
            return cache.putxIfAbsent(itemKey(t), true);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        try {
            return cache.removex(itemKey(o));
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> c) {
        for (Object obj : c) {
            if (!contains(obj))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<T> iterator() {
        try {
            GridCacheEnterpriseDataStructuresManager ds = (GridCacheEnterpriseDataStructuresManager) ctx.dataStructures();

            return ds.setIterator(this);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T1> T1[] toArray(T1[] a) {
        return null;
    }

    /**
     * @return Cache context.
     */
    GridCacheContext context() {
        return ctx;
    }

    /**
     * @param item Set item.
     * @return Item key.
     */
    private GridCacheSetItemKey itemKey(Object item) {
        return new GridCacheSetItemKey(name, id, item);
    }
}
