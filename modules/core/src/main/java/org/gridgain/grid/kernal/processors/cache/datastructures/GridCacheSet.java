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
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.Externalizable;
import java.util.*;

/**
 * Set implementation.
 */
public class GridCacheSet<T> extends AbstractCollection<T> implements Set<T> {
    /** */
    private static final int RMV_BATCH_SIZE = 100;

    /** */
    private final GridCacheContext ctx;

    /** */
    private final GridCache<GridCacheSetItemKey, Boolean> cache;

    /** */
    private final String name;

    /** */
    private final GridUuid id;

    /** */
    private final boolean collocated;

    /** */
    private final int hdrPart;

    /**
     * @param ctx Cache context.
     * @param name Set name.
     * @param hdr Set header.
     */
    @SuppressWarnings("unchecked")
    public GridCacheSet(GridCacheContext ctx, String name, GridCacheSetHeader hdr) {
        this.ctx = ctx;
        this.name = name;
        id = hdr.setId();
        collocated = hdr.collocated();

        cache = ctx.cache();

        hdrPart = ctx.affinity().partition(new GridCacheSetHeaderKey(name));
    }

    /**
     * @return Set ID.
     */
    public GridUuid id() {
        return id;
    }

    /**
     * @return Collocation flag.
     */
    boolean collocated() {
        return collocated;
    }

    /**
     * @param topVer Topology version.
     * @return Nodes where set data request should be sent.
     * @throws GridException If all cache nodes left grid.
     */
    @SuppressWarnings("unchecked")
    Collection<GridNode> dataNodes(long topVer) throws GridException {
        if (ctx.isLocal() || ctx.isReplicated())
            return Collections.singleton(ctx.localNode());

        Collection<GridNode> nodes;

        if (collocated) {
            nodes = ctx.affinity().nodes(hdrPart, topVer);

            if (!nodes.isEmpty())
                nodes = Collections.singleton(nodes.contains(ctx.localNode()) ? ctx.localNode() : F.first(nodes));
        }
        else
            nodes = CU.affinityNodes(ctx, topVer);

        if (nodes.isEmpty())
            throw new GridException("Failed to get set data, all cache nodes left grid.");

        return nodes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int size() {
        try {
            GridCacheEnterpriseDataStructuresManager ds = (GridCacheEnterpriseDataStructuresManager)ctx.dataStructures();

            GridFuture<Integer> fut = ds.setSize(this);

            return fut.get();
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        GridCacheEnterpriseDataStructuresManager ds = (GridCacheEnterpriseDataStructuresManager)ctx.dataStructures();

        return ds.setDataEmpty(id) && size() == 0;
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
        boolean add = false;

        for (T obj : c)
            add |= add(obj);

        return add;
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        boolean rmv = false;

        for (Object obj : c)
            rmv |= remove(obj);

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> c) {
        try {
            boolean rmv = false;

            List<GridCacheSetItemKey> rmvKeys = null;

            for (T val : this) {
                if (!c.contains(val)) {
                    rmv = true;

                    if (rmvKeys == null)
                        rmvKeys = new ArrayList<>(RMV_BATCH_SIZE);

                    rmvKeys.add(itemKey(val));

                    if (rmvKeys.size() == RMV_BATCH_SIZE) {
                        cache.removeAll(rmvKeys);

                        rmvKeys.clear();
                    }
                }
            }

            if (!F.isEmpty(rmvKeys))
                cache.removeAll(rmvKeys);

            return rmv;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            List<GridCacheSetItemKey> rmvKeys = new ArrayList<>(RMV_BATCH_SIZE);

            for (T val : this) {
                rmvKeys.add(itemKey(val));

                if (rmvKeys.size() == RMV_BATCH_SIZE) {
                    cache.removeAll(rmvKeys);

                    rmvKeys.clear();
                }
            }

            if (!rmvKeys.isEmpty())
                cache.removeAll(rmvKeys);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<T> iterator() {
        try {
            GridCacheEnterpriseDataStructuresManager ds = (GridCacheEnterpriseDataStructuresManager)ctx.dataStructures();

            return ds.setIterator(this);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
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
        return collocated ? new CollocatedItemKey(name, id, item) : new GridCacheSetItemKey(name, id, item);
    }

    /**
     * Item key for collocated set.
     */
    private static class CollocatedItemKey extends GridCacheSetItemKey {
        /**
         * Required by {@link Externalizable}.
         */
        public CollocatedItemKey() {
            // No-op.
        }

        /**
         * @param setName Set name.
         * @param setId Set unique ID.
         * @param item Set item.
         */
        private CollocatedItemKey(String setName, GridUuid setId, Object item) {
            super(setName, setId, item);
        }

        /**
         * @return Item affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return setName();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSet.class, this);
    }
}
