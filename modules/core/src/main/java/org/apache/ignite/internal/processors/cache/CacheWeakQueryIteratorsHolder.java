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

package org.apache.ignite.internal.processors.cache;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentHashMap8;

/**
 * @param <V> Type for cache query future.
 */
public class CacheWeakQueryIteratorsHolder<V> {
    /** Iterators weak references queue. */
    private final ReferenceQueue<WeakQueryFutureIterator> refQueue = new ReferenceQueue<>();

    /** Iterators futures. */
    private final Map<WeakReference<WeakQueryFutureIterator>,CacheQueryFuture<V>> futs =
        new ConcurrentHashMap8<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param log Logger.
     */
    public CacheWeakQueryIteratorsHolder(IgniteLogger log) {
        this.log = log;
    }

    /**
     * @param fut Query to iterate.
     * @param convert Cache iterator converter.
     * @param <T> Type for the iterator.
     * @return Iterator over the cache.
     */
    public <T> WeakQueryFutureIterator iterator(CacheQueryFuture<V> fut, CacheIteratorConverter<T, V> convert) {
        WeakQueryFutureIterator it = new WeakQueryFutureIterator(fut, convert);

        CacheQueryFuture<V> old = futs.put(it.weakReference(), fut);

        assert old == null;

        return it;
    }

    /**
     * @param it Iterator.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void removeIterator(WeakQueryFutureIterator it) throws IgniteCheckedException {
        futs.remove(it.weakReference());

        it.close();
    }

    /**
     * Closes unreachable iterators.
     */
    public void checkWeakQueue() {
        for (Reference<? extends WeakQueryFutureIterator> itRef = refQueue.poll(); itRef != null;
            itRef = refQueue.poll()) {
            try {
                WeakReference<WeakQueryFutureIterator> weakRef = (WeakReference<WeakQueryFutureIterator>)itRef;

                CacheQueryFuture<?> fut = futs.remove(weakRef);

                if (fut != null)
                    fut.cancel();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to close iterator.", e);
            }
        }
    }

    /**
     * Cancel all cache queries.
     */
    public void clearQueries(){
        for (CacheQueryFuture<?> fut : futs.values()) {
            try {
                fut.cancel();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to close iterator.", e);
            }
        }

        futs.clear();
    }


    /**
     * Iterator based of {@link CacheQueryFuture}.
     *
     * @param <T> Type for iterator.
     */
    public class WeakQueryFutureIterator<T> extends GridCloseableIteratorAdapter<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Query future. */
        private final CacheQueryFuture<V> fut;

        /** Weak reference. */
        private final WeakReference<WeakQueryFutureIterator<T>> weakRef;

        /** */
        private final CacheIteratorConverter<T, V> convert;

        /** Init flag. */
        private boolean init;

        /** Next item. */
        private T next;

        /** Current item. */
        private T cur;

        /**
         * @param fut GridCacheQueryFuture to iterate.
         * @param convert Converter.
         */
        WeakQueryFutureIterator(CacheQueryFuture<V> fut, CacheIteratorConverter<T, V> convert) {
            this.fut = fut;

            this.weakRef = new WeakReference<>(this, refQueue);

            this.convert = convert;
        }

        /** {@inheritDoc} */
        @Override public T onNext() throws IgniteCheckedException {
            init();

            if (next == null) {
                clearWeakReference();

                throw new NoSuchElementException();
            }

            cur = next;

            V futNext = fut.next();

            if (futNext == null)
                clearWeakReference();

            next = futNext != null ? convert.convert(futNext) : null;

            return cur;
        }

        /** {@inheritDoc} */
        @Override public boolean onHasNext() throws IgniteCheckedException {
            init();

            boolean hasNext = next != null;

            if (!hasNext)
                clearWeakReference();

            return hasNext;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            fut.cancel();

            clearWeakReference();
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() throws IgniteCheckedException {
            if (cur == null)
                throw new IllegalStateException();

            convert.remove(cur);

            cur = null;
        }

        /**
         * @return Iterator weak reference.
         */
        private WeakReference<WeakQueryFutureIterator<T>> weakReference() {
            return weakRef;
        }

        /**
         * Clears weak reference.
         */
        private void clearWeakReference() {
            weakRef.clear();

            futs.remove(weakRef);
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void init() throws IgniteCheckedException {
            if (!init) {
                V futNext = fut.next();

                next = futNext != null ? convert.convert(futNext) : null;

                init = true;
            }
        }
    }
}