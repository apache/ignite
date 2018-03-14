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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;

/** todo */
public class GridCacheSetAdapter<T> extends AbstractCollection<T> implements IgniteSet<T> {
    /** */
    private static final int BATCH_SIZE = 100;

    /** */
    private final IgniteCache<T, Object> cache;

    /** Logger. */
    private final IgniteLogger log;

    /** todo check useless */
    private final GridCacheContext<T, Object> ctx;

    /** */
    public GridCacheSetAdapter(IgniteCacheProxy<T, Object> delegate) {
        this.ctx = delegate.context();
        this.log = ctx.logger(this.getClass());
        this.cache = delegate;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T t) throws IgniteException {
        return cache.putIfAbsent(t, Boolean.TRUE);
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends T> c) throws IgniteException {
        boolean modified = false;

        Map<T, Boolean> addKeys = null;

        for (T obj : c) {
            if (modified) {
                if (addKeys == null)
                    addKeys = U.newHashMap(BATCH_SIZE);

                addKeys.put(obj, true);

                if (addKeys.size() == BATCH_SIZE) {
                    retryPutAll(addKeys);

                    addKeys.clear();
                }
            }
            else
                modified = add(obj);
        }

        if (!F.isEmpty(addKeys))
            retryPutAll(addKeys);

        return modified;
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteException {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) throws IgniteException {
        return cache.containsKey((T)o);
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> c) throws IgniteException {
        for (Object obj : c) {
            if (!contains(obj))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() throws IgniteException {
        // todo
        return cache.size() == 0;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() throws IgniteException {
        return iterator0();
    }

    /** */
    @SuppressWarnings("unchecked")
    private GridCloseableIterator<T> iterator0() {
        GridCacheContext ctx0 = ctx.isNear() ? ctx.near().dht().context() : ctx;

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        try {
            GridCloseableIterator<Map.Entry<T, Object>> iter = ctx0.queries()
                .createScanQuery(null, null, true)
                .keepAll(false)
                .executeScanQuery();

            return ctx.itHolder().iterator(iter, new CacheIteratorConverter<T, Map.Entry<T, Object>>() {
                @Override protected T convert(Map.Entry<T, Object> e) {
                    // Actually Scan Query returns Iterator<CacheQueryEntry> by default,
                    // CacheQueryEntry implements both Map.Entry and Cache.Entry interfaces.
                    return e.getKey();
                }

                @Override protected void remove(T item) {
                    CacheOperationContext prev = ctx.gate().enter(opCtx);

                    try {
                        cache.remove(item);
                    }
                    finally {
                        ctx.gate().leave(prev);
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) throws IgniteException {
        return cache.remove((T)o);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) throws IgniteException {
        boolean modified = false;

        Set<T> rmvKeys = null;

        for (Object obj : c) {
            if (modified) {
                if (rmvKeys == null)
                    rmvKeys = U.newHashSet(BATCH_SIZE);

                rmvKeys.add((T)obj);

                if (rmvKeys.size() == BATCH_SIZE) {
                    retryRemoveAll(rmvKeys);

                    rmvKeys.clear();
                }
            }
            else
                modified = remove(obj);
        }

        if (!F.isEmpty(rmvKeys))
            retryRemoveAll(rmvKeys);

        return modified;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> c) throws IgniteException {
        try {
            boolean modified = false;

            try (GridCloseableIterator<T> iter = iterator0()) {

                Set<T> rmvKeys = null;

                for (T val : iter) {
                    if (!c.contains(val)) {
                        modified = true;

                        if (rmvKeys == null)
                            rmvKeys = U.newHashSet(BATCH_SIZE);

                        rmvKeys.add((T)val);

                        if (rmvKeys.size() == BATCH_SIZE) {
                            retryRemoveAll(rmvKeys);

                            rmvKeys.clear();
                        }
                    }
                }

                if (!F.isEmpty(rmvKeys))
                    retryRemoveAll(rmvKeys);
            }

            return modified;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteException {
        return cache.size();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        cache.destroy();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.getName();
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        // todo check
        return cache.isClosed();
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(IgniteRunnable job) throws IgniteException {
        throw new IgniteException("Failed to execute affinityRun() for non-collocated set: " + name() +
            ". This operation is supported only for collocated sets.");
    }
    /**
     * {@inheritDoc} */
    @Override public <R> R affinityCall(IgniteCallable<R> job) throws IgniteException {
        throw new IgniteException("Failed to execute affinityCall() for non-collocated set: " + name() +
            ". This operation is supported only for collocated sets.");
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
    private void retryRemoveAll(final Set<T> keys) {
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
    private void retryPutAll(final Map<T, Boolean> keys) {
        retry(new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.putAll(keys);

                return null;
            }
        });
    }
}
