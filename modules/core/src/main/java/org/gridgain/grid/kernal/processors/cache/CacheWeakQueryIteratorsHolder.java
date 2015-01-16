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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.*;
import org.jdk8.backport.*;

import java.lang.ref.*;
import java.util.*;

/**
 * @param <T> Type for iterator.
 * @param <V> Type for cache query future.
 */
public abstract class CacheWeakQueryIteratorsHolder<T, V> {
    /** Iterators weak references queue. */
    private final ReferenceQueue<WeakQueryFutureIterator> refQueue = new ReferenceQueue<>();

    /** Iterators futures. */
    private final Map<WeakReference<WeakQueryFutureIterator>, GridCacheQueryFuture<V>> futs =
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
     * Iterator over the cache.
     * @param fut Query to iterate
     * @return iterator
     */
    public WeakQueryFutureIterator iterator(GridCacheQueryFuture<V> fut) {
        WeakQueryFutureIterator it = new WeakQueryFutureIterator(fut);

        futs.put(it.weakReference(), fut);

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
        for (Reference<? extends WeakQueryFutureIterator> itRef = refQueue.poll(); itRef != null; itRef = refQueue.poll()) {
            try {
                WeakReference<WeakQueryFutureIterator> weakRef = (WeakReference<WeakQueryFutureIterator>)itRef;

                GridCacheQueryFuture<?> fut = futs.remove(weakRef);

                if (fut != null)
                    fut.cancel();
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to close iterator.", e);
            }
        }
    }

    /**
     * Cancel all cache queries
     */
    public void clearQueries(){
        for (GridCacheQueryFuture<?> fut : futs.values()) {
            try {
                fut.cancel();
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to close iterator.", e);
            }
        }

        futs.clear();
    }

    /**
     * Converts class V to class T.
     *
     * @param v Item to convert.
     * @return Converted item.
     */
    protected abstract T convert(V v);

    /**
     * Removes item.
     *
     * @param item Item to remove.
     */
    protected abstract void remove(T item);

    /**
     * Iterator based of {@link GridCacheQueryFuture}.
     */
    public class WeakQueryFutureIterator extends GridCloseableIteratorAdapter<T> {
        /** Query future. */
        private final GridCacheQueryFuture<V> fut;

        /** Weak reference. */
        private final WeakReference<WeakQueryFutureIterator> weakRef;

        /** Init flag. */
        private boolean init;

        /** Next item. */
        private T next;

        /** Current item. */
        private T cur;

        /**
         * @param fut GridCacheQueryFuture to iterate.
         */
        WeakQueryFutureIterator(GridCacheQueryFuture<V> fut) {
            this.fut = fut;

            this.weakRef = new WeakReference<>(this, refQueue);
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

            next = futNext != null ? convert(futNext) : null;

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

            CacheWeakQueryIteratorsHolder.this.remove(cur);

            cur = null;
        }

        /**
         * @return Iterator weak reference.
         */
        private WeakReference<WeakQueryFutureIterator> weakReference() {
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

                next = futNext != null ? convert(futNext) : null;

                init = true;
            }
        }
    }
}
