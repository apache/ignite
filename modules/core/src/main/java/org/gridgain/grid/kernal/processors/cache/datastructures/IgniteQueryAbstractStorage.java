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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.jdk8.backport.*;

import java.lang.ref.*;
import java.util.*;

/**
 * Storage for GridCacheQueryFuture.
 * @param <T> Type for iterator.
 * @param <V> Type for cache query future.
 */
public abstract class IgniteQueryAbstractStorage<T, V> {
    /** Iterators weak references queue. */
    private final ReferenceQueue<IgniteIterator> refQueue = new ReferenceQueue<>();

    /** Iterators futures. */
    private final Map<WeakReference<IgniteIterator>, GridCacheQueryFuture<V>> futs = new ConcurrentHashMap8<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Cache context.
     */
    public IgniteQueryAbstractStorage(GridCacheContext ctx) {
        log = ctx.logger(IgniteQueryAbstractStorage.class);
    }

    /**
     * Iterator over the cache.
     * @param fut Query to iterate
     * @return iterator
     */
    public IgniteIterator iterator(GridCacheQueryFuture<V> fut) {
        IgniteIterator it = new IgniteIterator(fut);

        futs.put(it.weakReference(), fut);

        return it;
    }

    public void removeIterator(IgniteIterator it) throws IgniteCheckedException {
        futs.remove(it.weakReference());

        it.close();
    }

    /**
     * Closes unreachable iterators.
     */
    public void checkWeakQueue() {
        for (Reference<? extends IgniteIterator> itRef = refQueue.poll(); itRef != null; itRef = refQueue.poll()) {
            try {
                WeakReference<IgniteIterator> weakRef = (WeakReference<IgniteIterator>)itRef;

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
     * Checks if set was removed and handles iterators weak reference queue.
     */
    public void onAccess() throws IgniteCheckedException {
        checkWeakQueue();
    }

    /**
     * Cancel all cache queries
     */
    protected void clearQueries(){
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
     * Convert class V to class T.
     * @param v Item to convert.
     * @return Converted item.
     */
    protected abstract T convert(V v);

    /**
     * Remove item from the cache.
     * @param item Item to remove.
     */
    protected abstract void remove(T item);

    /**
     * Iterator over the cache.
     */
    public class IgniteIterator extends GridCloseableIteratorAdapter<T> {
        /** Query future. */
        private final GridCacheQueryFuture<V> fut;

        /** Weak reference. */
        private final WeakReference<IgniteIterator> weakRef;

        /** Init flag. */
        private boolean init;

        /** Next item. */
        private T next;

        /** Current item. */
        private T cur;

        /**
         * @param fut GridCacheQueryFuture to iterate
         */
        IgniteIterator(GridCacheQueryFuture<V> fut) {
            this.fut = fut;

            this.weakRef = new WeakReference<IgniteIterator>(this, refQueue);
        }

        /**
         * @return Iterator weak reference.
         */
        public WeakReference<IgniteIterator> weakReference() {
            return weakRef;
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

            IgniteQueryAbstractStorage.this.remove(cur);

            cur = null;
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
