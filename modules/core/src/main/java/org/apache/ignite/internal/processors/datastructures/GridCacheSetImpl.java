/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.CacheWeakQueryIteratorsHolder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SET;

/**
 * Cache set implementation.
 */
public class GridCacheSetImpl<T> extends AbstractCollection<T> implements IgniteSet<T> {
    /** */
    private static final int BATCH_SIZE = 100;

    /** Cache context. */
    private final GridCacheContext ctx;

    /** Cache. */
    private final IgniteInternalCache<SetItemKey, Boolean> cache;

    /** Logger. */
    private final IgniteLogger log;

    /** Set name. */
    private final String name;

    /** Set unique ID. */
    private final IgniteUuid id;

    /** Collocation flag. */
    private final boolean collocated;

    /** Set header partition. */
    private final int hdrPart;

    /** Set header key. */
    protected final GridCacheSetHeaderKey setKey;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** */
    private final boolean binaryMarsh;

    /** Access to affinityRun() and affinityCall() functions. */
    private final IgniteCompute compute;

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
        binaryMarsh = ctx.binaryMarshaller();
        compute = ctx.kernalContext().grid().compute();

        cache = ctx.cache();

        setKey = new GridCacheSetHeaderKey(name);

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

    /**
     * @return {@code True} if set header found in cache.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public boolean checkHeader() throws IgniteCheckedException {
        IgniteInternalCache<GridCacheSetHeaderKey, GridCacheSetHeader> cache0 = ctx.cache();

        GridCacheSetHeader hdr = cache0.get(new GridCacheSetHeaderKey(name));

        return hdr != null && hdr.id().equals(id);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int size() {
        try {
            onAccess();

            if (ctx.isLocal() || ctx.isReplicated()) {
                GridConcurrentHashSet<SetItemKey> set = ctx.dataStructures().setData(id);

                return set != null ? set.size() : 0;
            }

            CacheQuery qry = new GridCacheQueryAdapter<>(ctx, SET, null, null,
                new GridSetQueryPredicate<>(id, collocated), null, false, false);

            Collection<ClusterNode> nodes = dataNodes(ctx.affinity().affinityTopologyVersion());

            qry.projection(ctx.grid().cluster().forNodes(nodes));

            Iterable<Integer> col = (Iterable<Integer>)qry.execute(new SumReducer()).get();

            int sum = 0;

            for (Integer val : col)
                sum += val;

            return sum;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean isEmpty() {
        onAccess();

        GridConcurrentHashSet<SetItemKey> set = ctx.dataStructures().setData(id);

        return (set == null || set.isEmpty()) && size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        onAccess();

        final SetItemKey key = itemKey(o);

        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache.get(key) != null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean add(T o) {
        onAccess();

        final SetItemKey key = itemKey(o);

        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache.putIfAbsent(key, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        onAccess();

        final SetItemKey key = itemKey(o);

        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache.remove(key);
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

        Map<SetItemKey, Boolean> addKeys = null;

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
                add = add(obj);
        }

        if (!F.isEmpty(addKeys))
            retryPutAll(addKeys);

        return add;
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        onAccess();

        boolean rmv = false;

        Set<SetItemKey> rmvKeys = null;

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
                rmv = remove(obj);
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

                Set<SetItemKey> rmvKeys = null;

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
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            onAccess();

            try (GridCloseableIterator<T> iter = iterator0()) {
                Collection<SetItemKey> rmvKeys = new ArrayList<>(BATCH_SIZE);

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
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        onAccess();

        return iterator0();
    }

    /** {@inheritDoc} */
    public void affinityRun(IgniteRunnable job) {
        if (!collocated)
            throw new IgniteException("Failed to execute affinityRun() for non-collocated set: " + name() +
                ". This operation is supported only for collocated sets.");

        compute.affinityRun(cache.name(), setKey, job);
    }

    /** {@inheritDoc} */
    public <R> R affinityCall(IgniteCallable<R> job) {
        if (!collocated)
            throw new IgniteException("Failed to execute affinityCall() for non-collocated set: " + name() +
                ". This operation is supported only for collocated sets.");

        return compute.affinityCall(cache.name(), setKey, job);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            if (rmvd)
                return;

            ctx.kernalContext().dataStructures().removeSet(name, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    private GridCloseableIterator<T> iterator0() {
        try {
            CacheQuery qry = new GridCacheQueryAdapter<>(ctx, SET, null, null,
                new GridSetQueryPredicate<>(id, collocated), null, false, false);

            Collection<ClusterNode> nodes = dataNodes(ctx.affinity().affinityTopologyVersion());

            qry.projection(ctx.grid().cluster().forNodes(nodes));

            CacheQueryFuture<Map.Entry<T, ?>> fut = qry.execute();

            CacheWeakQueryIteratorsHolder.WeakReferenceCloseableIterator it =
                ctx.itHolder().iterator(fut, new CacheIteratorConverter<T, Map.Entry<T, ?>>() {
                    @Override protected T convert(Map.Entry<T, ?> e) {
                        return e.getKey();
                    }

                    @Override protected void remove(T item) {
                        GridCacheSetImpl.this.remove(item);
                    }
                });

            if (rmvd) {
                ctx.itHolder().removeIterator(it);

                checkRemoved();
            }

            return it;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @param call Callable.
     * @return Callable result.
     */
    private <R> R retry(Callable<R> call) {
        try {
            return DataStructuresProcessor.retry(log, call);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @param keys Keys to remove.
     */
    private void retryRemoveAll(final Collection<SetItemKey> keys) {
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
    private void retryPutAll(final Map<SetItemKey, Boolean> keys) {
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
     * @throws IgniteCheckedException If all cache nodes left grid.
     */
    @SuppressWarnings("unchecked")
    private Collection<ClusterNode> dataNodes(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        if (ctx.isLocal() || ctx.isReplicated())
            return Collections.singleton(ctx.localNode());

        Collection<ClusterNode> nodes;

        if (collocated) {
            List<ClusterNode> nodes0 = ctx.affinity().nodesByPartition(hdrPart, topVer);

            nodes = !nodes0.isEmpty() ?
                Collections.singleton(nodes0.contains(ctx.localNode()) ? ctx.localNode() : F.first(nodes0)) : nodes0;
        }
        else
            nodes = CU.affinityNodes(ctx, topVer);

        if (nodes.isEmpty())
            throw new IgniteCheckedException("Failed to get set data, all cache nodes left grid.");

        return nodes;
    }

    /**
     * @param rmvd Removed flag.
     */
    void removed(boolean rmvd) {
        if (this.rmvd)
            return;

        this.rmvd = rmvd;

        if (rmvd)
            ctx.itHolder().clearQueries();
    }

    /**
     * Throws {@link IllegalStateException} if set was removed.
     */
    private void checkRemoved() {
        if (rmvd)
            throw new IllegalStateException("Set has been removed from cache: " + this);
    }

    /**
     * Checks if set was removed and handles iterators weak reference queue.
     */
    private void onAccess() {
        ctx.itHolder().checkWeakQueue();

        checkRemoved();
    }

    /**
     * @return Set ID.
     */
    public IgniteUuid id() {
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
    private SetItemKey itemKey(Object item) {
        return collocated ? (binaryMarsh ? new CollocatedSetItemKey(name, id, item) : new CollocatedItemKey(name, id, item))
            : new GridCacheSetItemKey(id, item);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetImpl.class, this);
    }

    /**
     *
     */
    private static class SumReducer implements IgniteReducer<Object, Integer>, Externalizable {
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
        private CollocatedItemKey(String setName, IgniteUuid setId, Object item) {
            super(setId, item);

            this.setName = setName;
        }

        /**
         * @return Item affinity key.
         */
        @AffinityKeyMapped
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
