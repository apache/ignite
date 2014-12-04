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
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.cache.query.GridCacheQueryType.*;

/**
 * Cache set implementation.
 */
public class GridCacheSetImpl<T> extends AbstractCollection<T> implements GridCacheSet<T> {
    /** */
    private static final int BATCH_SIZE = 100;

    /** Cache context. */
    private final GridCacheContext ctx;

    /** Cache. */
    private final GridCache<GridCacheSetItemKey, Boolean> cache;

    /** Logger. */
    private final GridLogger log;

    /** Set name. */
    private final String name;

    /** Set unique ID. */
    private final GridUuid id;

    /** Collocation flag. */
    private final boolean collocated;

    /** Queue header partition. */
    private final int hdrPart;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** Iterators weak references queue. */
    private final ReferenceQueue<SetIterator<?>> itRefQueue = new ReferenceQueue<>();

    /** Iterators futures. */
    private final Map<WeakReference<SetIterator<?>>, GridCacheQueryFuture<?>> itFuts = new ConcurrentHashMap8<>();

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

        log = ctx.logger(GridCacheSetImpl.class);

        hdrPart = ctx.affinity().partition(new GridCacheSetHeaderKey(name));
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        return collocated;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int size() {
        try {
            onAccess();

            if (ctx.isLocal() || ctx.isReplicated()) {
                GridConcurrentHashSet<GridCacheSetItemKey> set = ctx.dataStructures().setData(id);

                return set != null ? set.size() : 0;
            }

            GridCacheQuery qry = new GridCacheQueryAdapter<>(ctx, SET, null, null, null,
                new GridSetQueryPredicate<>(id, collocated), false, false);

            Collection<ClusterNode> nodes = dataNodes(ctx.affinity().affinityTopologyVersion());

            qry.projection(ctx.grid().forNodes(nodes));

            Iterable<Integer> col = (Iterable<Integer>) qry.execute(new SumReducer()).get();

            int sum = 0;

            for (Integer val : col)
                sum += val;

            return sum;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean isEmpty() {
        onAccess();

        GridConcurrentHashSet<GridCacheSetItemKey> set = ctx.dataStructures().setData(id);

        return (set == null || set.isEmpty()) && size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        onAccess();

        final GridCacheSetItemKey key = itemKey(o);

        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache.get(key) != null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean add(T o) {
        onAccess();

        final GridCacheSetItemKey key = itemKey(o);

        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache.putxIfAbsent(key, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        onAccess();

        final GridCacheSetItemKey key = itemKey(o);

        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache.removex(key);
            }
        });
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
        onAccess();

        boolean add = false;

        Map<GridCacheSetItemKey, Boolean> addKeys = null;

        for (T obj : c) {
            if (add) {
                if (addKeys == null)
                    addKeys = U.newHashMap(BATCH_SIZE);

                addKeys.put(itemKey(obj), true);

                if (addKeys.size() == BATCH_SIZE) {
                    retryPutAll(addKeys);

                    addKeys.clear();
                }
            }
            else
                add |= add(obj);
        }

        if (!F.isEmpty(addKeys))
            retryPutAll(addKeys);

        return add;
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        onAccess();

        boolean rmv = false;

        Set<GridCacheSetItemKey> rmvKeys = null;

        for (Object obj : c) {
            if (rmv) {
                if (rmvKeys == null)
                    rmvKeys = U.newHashSet(BATCH_SIZE);

                rmvKeys.add(itemKey(obj));

                if (rmvKeys.size() == BATCH_SIZE) {
                    retryRemoveAll(rmvKeys);

                    rmvKeys.clear();
                }
            }
            else
                rmv |= remove(obj);
        }

        if (!F.isEmpty(rmvKeys))
            retryRemoveAll(rmvKeys);

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> c) {
        try {
            onAccess();

            try (GridCloseableIterator<T> iter = iterator0()) {
                boolean rmv = false;

                Set<GridCacheSetItemKey> rmvKeys = null;

                for (T val : iter) {
                    if (!c.contains(val)) {
                        rmv = true;

                        if (rmvKeys == null)
                            rmvKeys = U.newHashSet(BATCH_SIZE);

                        rmvKeys.add(itemKey(val));

                        if (rmvKeys.size() == BATCH_SIZE) {
                            retryRemoveAll(rmvKeys);

                            rmvKeys.clear();
                        }
                    }
                }

                if (!F.isEmpty(rmvKeys))
                    retryRemoveAll(rmvKeys);

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
            onAccess();

            try (GridCloseableIterator<T> iter = iterator0()) {
                Collection<GridCacheSetItemKey> rmvKeys = new ArrayList<>(BATCH_SIZE);

                for (T val : iter) {
                    rmvKeys.add(itemKey(val));

                    if (rmvKeys.size() == BATCH_SIZE) {
                        retryRemoveAll(rmvKeys);

                        rmvKeys.clear();
                    }
                }

                if (!rmvKeys.isEmpty())
                    retryRemoveAll(rmvKeys);
            }
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        onAccess();

        return iterator0();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    private GridCloseableIterator<T> iterator0() {
        try {
            GridCacheQuery qry = new GridCacheQueryAdapter<>(ctx, SET, null, null, null,
                new GridSetQueryPredicate<>(id, collocated), false, false);

            Collection<ClusterNode> nodes = dataNodes(ctx.affinity().affinityTopologyVersion());

            qry.projection(ctx.grid().forNodes(nodes));

            GridCacheQueryFuture<T> fut = qry.execute();

            SetIterator<T> it = new SetIterator<>(fut);

            itFuts.put(it.weakReference(), fut);

            if (rmvd) {
                itFuts.remove(it.weakReference());

                it.close();

                checkRemoved();
            }

            return it;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }
    /**
     * @param call Callable.
     * @return Callable result.
     */
    private <R> R retry(Callable<R> call) {
        try {
            return (R)ctx.dataStructures().retry(call);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * @param keys Keys to remove.
     */
    private void retryRemoveAll(final Collection<GridCacheSetItemKey> keys) {
        retry(new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(keys);

                return null;
            }
        });
    }

    /**
     * @param keys Keys to remove.
     */
    private void retryPutAll(final Map<GridCacheSetItemKey, Boolean> keys) {
        retry(new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.putAll(keys);

                return null;
            }
        });
    }

    /**
     * @param topVer Topology version.
     * @return Nodes where set data request should be sent.
     * @throws GridException If all cache nodes left grid.
     */
    @SuppressWarnings("unchecked")
    private Collection<ClusterNode> dataNodes(long topVer) throws GridException {
        if (ctx.isLocal() || ctx.isReplicated())
            return Collections.singleton(ctx.localNode());

        Collection<ClusterNode> nodes;

        if (collocated) {
            List<ClusterNode> nodes0 = ctx.affinity().nodes(hdrPart, topVer);

            nodes = !nodes0.isEmpty() ?
                Collections.singleton(nodes0.contains(ctx.localNode()) ? ctx.localNode() : F.first(nodes0)) : nodes0;
        }
        else
            nodes = CU.affinityNodes(ctx, topVer);

        if (nodes.isEmpty())
            throw new GridException("Failed to get set data, all cache nodes left grid.");

        return nodes;
    }

    /**
     * @param rmvd Removed flag.
     */
    void removed(boolean rmvd) {
        if (this.rmvd)
            return;

        this.rmvd = rmvd;

        if (rmvd) {
            for (GridCacheQueryFuture<?> fut : itFuts.values()) {
                try {
                    fut.cancel();
                }
                catch (GridException e) {
                    log.error("Failed to close iterator.", e);
                }
            }

            itFuts.clear();
        }
    }

    /**
     * Throws {@link GridCacheDataStructureRemovedRuntimeException} if set was removed.
     */
    private void checkRemoved() {
        if (rmvd)
            throw new GridCacheDataStructureRemovedRuntimeException("Set has been removed from cache: " + this);
    }

    /**
     * Closes unreachable iterators.
     */
    private void checkWeakQueue() {
        for (Reference<? extends SetIterator<?>> itRef = itRefQueue.poll(); itRef != null; itRef = itRefQueue.poll()) {
            try {
                WeakReference<SetIterator<?>> weakRef = (WeakReference<SetIterator<?>>)itRef;

                GridCacheQueryFuture<?> fut = itFuts.remove(weakRef);

                if (fut != null)
                    fut.cancel();
            }
            catch (GridException e) {
                log.error("Failed to close iterator.", e);
            }
        }
    }

    /**
     * Checks if set was removed and handles iterators weak reference queue.
     */
    private void onAccess() {
        checkWeakQueue();

        checkRemoved();
    }

    /**
     * @return Set ID.
     */
    GridUuid id() {
        return id;
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
        return collocated ? new CollocatedItemKey(name, id, item) : new GridCacheSetItemKey(id, item);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetImpl.class, this);
    }

    /**
     *
     */
    private class SetIterator<T> extends GridCloseableIteratorAdapter<T> {
        /** */
        private static final long serialVersionUID = -1460570789166994846L;

        /** Query future. */
        private final GridCacheQueryFuture<T> fut;

        /** Init flag. */
        private boolean init;

        /** Next item. */
        private T next;

        /** Current item. */
        private T cur;

        /** Weak reference. */
        private final WeakReference<SetIterator<?>> weakRef;

        /**
         * @param fut Query future.
         */
        private SetIterator(GridCacheQueryFuture<T> fut) {
            this.fut = fut;

            weakRef = new WeakReference<SetIterator<?>>(this, itRefQueue);
        }

        /** {@inheritDoc} */
        @Override protected T onNext() throws GridException {
            init();

            if (next == null) {
                clearWeakReference();

                throw new NoSuchElementException();
            }

            cur = next;

            Map.Entry e = (Map.Entry)fut.next();

            next = e != null ? (T)e.getKey() : null;

            if (next == null)
                clearWeakReference();

            return cur;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws GridException {
            init();

            boolean hasNext = next != null;

            if (!hasNext)
                clearWeakReference();

            return hasNext;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws GridException {
            fut.cancel();

            clearWeakReference();
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() throws GridException {
            if (cur == null)
                throw new NoSuchElementException();

            GridCacheSetImpl.this.remove(cur);
        }

        /**
         * @throws GridException If failed.
         */
        private void init() throws GridException {
            if (!init) {
                Map.Entry e = (Map.Entry)fut.next();

                next = e != null ? (T)e.getKey() : null;

                init = true;
            }
        }

        /**
         * @return Iterator weak reference.
         */
        WeakReference<SetIterator<?>> weakReference() {
            return weakRef;
        }

        /**
         * Clears weak reference.
         */
        private void clearWeakReference() {
            weakRef.clear(); // Do not need to enqueue.

            itFuts.remove(weakRef);
        }
    }

    /**
     *
     */
    private static class SumReducer implements GridReducer<Object, Integer>, Externalizable {
        /** */
        private static final long serialVersionUID = -3436987759126521204L;

        /** */
        private int cntr;

        /**
         * Required by {@link Externalizable}.
         */
        public SumReducer() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable Object o) {
            cntr++;

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return cntr;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    /**
     * Item key for collocated set.
     */
    private static class CollocatedItemKey extends GridCacheSetItemKey {
        /** */
        private static final long serialVersionUID = -1400701398705953750L;

        /** */
        private String setName;

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
            super(setId, item);

            this.setName = setName;
        }

        /**
         * @return Item affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return setName;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);

            U.writeString(out, setName);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);

            setName = U.readString(in);
        }
    }
}
