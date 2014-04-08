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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Set implementation.
 */
public class GridCacheSetImpl<T> extends AbstractCollection<T> implements GridCacheSet<T> {
    /** */
    private static final int BATCH_SIZE = 100;

    /** Cache context. */
    private final GridCacheContext ctx;

    /** Cache. */
    private final GridCache<GridCacheSetItemKey, Boolean> cache;

    /** Set name. */
    private final String name;

    /** Set unique ID. */
    private final GridUuid id;

    /** Collocation flag. */
    private final boolean collocated;

    /** Queue header partition. */
    private final int hdrPart;

    /**
     * @param ctx Cache context.
     * @param name Set name.
     * @param hdr Set header.
     */
    @SuppressWarnings("unchecked")
    public GridCacheSetImpl(GridCacheContext ctx, String name, GridCacheSetHeader hdr) {
        this.ctx = ctx;
        this.name = name;
        id = hdr.id();
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

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        return collocated;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
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
        try {
            boolean add = false;

            Map<GridCacheSetItemKey, Boolean> addKeys = null;

            for (T obj : c) {
                if (add) {
                    if (addKeys == null)
                        addKeys = new HashMap<>(BATCH_SIZE);

                    addKeys.put(itemKey(obj), true);

                    if (addKeys.size() == BATCH_SIZE) {
                        cache.putAll(addKeys);

                        addKeys.clear();
                    }
                }
                else
                    add |= add(obj);
            }

            if (!F.isEmpty(addKeys))
                cache.putAll(addKeys);

            return add;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        try {
            boolean rmv = false;

            Set<GridCacheSetItemKey> rmvKeys = null;

            for (Object obj : c) {
                if (rmv) {
                    if (rmvKeys == null)
                        rmvKeys = new HashSet<>(BATCH_SIZE);

                    rmvKeys.add(itemKey(obj));

                    if (rmvKeys.size() == BATCH_SIZE) {
                        cache.removeAll(rmvKeys);

                        rmvKeys.clear();
                    }
                }
                else
                    rmv |= remove(obj);
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
    @Override public boolean retainAll(Collection<?> c) {
        try {
            try (GridCloseableIterator<T> iter = iteratorEx()) {
                boolean rmv = false;

                Set<GridCacheSetItemKey> rmvKeys = null;

                for (T val : iter) {
                    if (!c.contains(val)) {
                        rmv = true;

                        if (rmvKeys == null)
                            rmvKeys = new HashSet<>(BATCH_SIZE);

                        rmvKeys.add(itemKey(val));

                        if (rmvKeys.size() == BATCH_SIZE) {
                            cache.removeAll(rmvKeys);

                            rmvKeys.clear();
                        }
                    }
                }

                if (!F.isEmpty(rmvKeys))
                    cache.removeAll(rmvKeys);

                return rmv;
            }
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            try (GridCloseableIterator<T> iter = iteratorEx()) {
                List<GridCacheSetItemKey> rmvKeys = new ArrayList<>(BATCH_SIZE);

                for (T val : iter) {
                    rmvKeys.add(itemKey(val));

                    if (rmvKeys.size() == BATCH_SIZE) {
                        cache.removeAll(rmvKeys);

                        rmvKeys.clear();
                    }
                }

                if (!rmvKeys.isEmpty())
                    cache.removeAll(rmvKeys);
            }
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        return iteratorEx();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCloseableIterator<T> iteratorEx() {
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
        return S.toString(GridCacheSetImpl.class, this);
    }
}
