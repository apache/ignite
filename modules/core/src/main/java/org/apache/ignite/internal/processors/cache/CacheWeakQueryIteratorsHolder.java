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
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentHashMap8;

/**
 * @param <V> Type for cache query future.
 */
public class CacheWeakQueryIteratorsHolder<V> {
    /** Iterators weak references queue. */
    private final ReferenceQueue refQueue = new ReferenceQueue();

    /** Iterators futures. */
    private final Map<WeakReference, AutoCloseable> refs = new ConcurrentHashMap8<>();

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
    public <T> WeakReferenceCloseableIterator<T> iterator(final CacheQueryFuture<V> fut,
        CacheIteratorConverter<T, V> convert) {
        WeakQueryFutureIterator it = new WeakQueryFutureIterator(fut, convert);

        AutoCloseable old = refs.put(it.weakReference(), fut);

        assert old == null;

        return it;
    }

    /**
     * @param iter Closeable iterator.
     * @param <T> Type for the iterator.
     * @return Iterator over the cache.
     */
    public <T> WeakReferenceCloseableIterator<T> iterator(final GridCloseableIterator<V> iter,
        CacheIteratorConverter<T, V> convert) {
        WeakQueryCloseableIterator it = new WeakQueryCloseableIterator(iter, convert);

        AutoCloseable old = refs.put(it.weakReference(), iter);

        assert old == null;

        return it;
    }

    /**
     * @param it Iterator.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void removeIterator(WeakReferenceCloseableIterator it) throws IgniteCheckedException {
        refs.remove(it.weakReference());

        it.close();
    }

    /**
     * Closes unreachable iterators.
     */
    public void checkWeakQueue() {
        for (Reference itRef = refQueue.poll(); itRef != null;
            itRef = refQueue.poll()) {
            try {
                WeakReference weakRef = (WeakReference)itRef;

                AutoCloseable rsrc = refs.remove(weakRef);

                if (rsrc != null)
                    rsrc.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to close iterator.", e);
            }
        }
    }

    /**
     * Cancel all cache queries.
     */
    public void clearQueries(){
        for (AutoCloseable rsrc : refs.values()) {
            try {
                rsrc.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to close iterator.", e);
            }
        }

        refs.clear();
    }


    /**
     * Iterator based of {@link CacheQueryFuture}.
     *
     * @param <T> Type for iterator.
     */
    private class WeakQueryFutureIterator<T> extends GridCloseableIteratorAdapter<T>
        implements WeakReferenceCloseableIterator<T> {
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

        /** {@inheritDoc} */
        @Override public WeakReference<WeakQueryFutureIterator<T>> weakReference() {
            return weakRef;
        }

        /**
         * Clears weak reference.
         */
        private void clearWeakReference() {
            weakRef.clear();

            refs.remove(weakRef);
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

    /**
     * @param <T> Type.
     */
    public class WeakQueryCloseableIterator<T> extends GridCloseableIteratorAdapter<T>
        implements WeakReferenceCloseableIterator<T> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private final GridCloseableIterator<V> iter;

        /** */
        private final CacheIteratorConverter<T, V> convert;

        /** */
        private final WeakReference weakRef;

        /** */
        private T cur;

        /**
         * @param iter Iterator.
         * @param convert Converter.
         */
        WeakQueryCloseableIterator(GridCloseableIterator<V> iter, CacheIteratorConverter<T, V> convert) {
            this.iter = iter;
            this.convert = convert;

            weakRef = new WeakReference(this, refQueue);
       }


        /** {@inheritDoc} */
        @Override protected T onNext() throws IgniteCheckedException {
            V next;

            try {
                next = iter.nextX();
            }
            catch (NoSuchElementException e){
                clearWeakReference();

                throw e;
            }

            if (next == null)
                clearWeakReference();

            cur = next != null ? convert.convert(next) : null;

            return cur;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            boolean hasNextX = iter.hasNextX();

            if (!hasNextX)
                clearWeakReference();

            return hasNextX;
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() throws IgniteCheckedException {
            if (cur == null)
                throw new IllegalStateException();

            convert.remove(cur);

            cur = null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            iter.close();

            clearWeakReference();
        }

        /**
         * Clears weak reference.
         */
        private void clearWeakReference() {
            weakRef.clear();

            refs.remove(weakRef);
        }

        /** {@inheritDoc} */
        @Override public WeakReference weakReference() {
            return weakRef;
        }
    }

    /**
     *
     */
    public static interface WeakReferenceCloseableIterator<T> extends GridCloseableIterator<T> {
        /**
         * @return Iterator weak reference.
         */
        public WeakReference weakReference();
    }
}
