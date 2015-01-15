package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jdk8.backport.*;

import java.lang.ref.*;
import java.util.*;

/**
 * Storage for GridCacheQueryFuture.
 */
public class IgniteQueryFutureStorage {
    /** Iterators weak references queue. */
    private final ReferenceQueue<Iterator<?>> refQueue = new ReferenceQueue<>();

    /** Iterators futures. */
    private final Map<WeakReference<Iterator<?>>, GridCacheQueryFuture<?>> futs = new ConcurrentHashMap8<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Cache context.
     */
    public IgniteQueryFutureStorage(GridCacheContext ctx) {
        log = ctx.logger(GridCacheSetImpl.class);
    }

    /**
     * Iterator over the cache.
     * @param fut Query to iterate
     * @return iterator
     */
    public <T> Iterator<T> iterator(GridCacheQueryFuture<T> fut) {
        Iterator<T> it = new Iterator<>(fut);

        futs.put(it.weakReference(), fut);

        return it;
    }

    /**
     * Closes unreachable iterators.
     */
    private void checkWeakQueue() throws IgniteCheckedException {
        for (Reference<? extends Iterator<?>> itRef = refQueue.poll(); itRef != null; itRef = refQueue.poll()) {
            WeakReference<Iterator<?>> weakRef = (WeakReference<Iterator<?>>) itRef;

            GridCacheQueryFuture<?> fut = futs.remove(weakRef);

            if (fut != null)
                fut.cancel();

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
     * @throws IgniteCheckedException
     */
    protected void clearQueries() throws IgniteCheckedException {
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
     * Iterator over the cache
     */
    public class Iterator<T> {
        /** Query future. */
        private final GridCacheQueryFuture<T> fut;

        /** Weak reference. */
        private final WeakReference<Iterator<?>> weakRef;

        /** Init flag. */
        private boolean init;

        /** Next item. */
        private T next;

        /** Current item. */
        private T cur;

        /**
         * @param fut GridCacheQueryFuture to iterate
         */
        Iterator(GridCacheQueryFuture<T> fut) {
            this.fut = fut;
            this.weakRef = new WeakReference<Iterator<?>>(this, refQueue);
        }


        /**
         * @throws IgniteCheckedException If failed.
         */
        private void init() throws IgniteCheckedException {
            if (!init) {
                next = fut.next();

                init = true;
            }
        }

        /**
         * @return Iterator weak reference.
         */
        WeakReference<Iterator<?>> weakReference() {
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
         * The same as Iterator.next()
         */
        public T onNext() throws IgniteCheckedException {
            init();

            if (next == null) {
                clearWeakReference();

                throw new NoSuchElementException();
            }

            cur = next;
            next = fut.next();

            if (next == null)
                clearWeakReference();

            return cur;
        }

        /**
         * The same as Iterator.hasNext()
         */
        public boolean onHasNext() throws IgniteCheckedException {
            init();

            boolean hasNext = next != null;

            if (!hasNext)
                clearWeakReference();

            return hasNext;
        }

        /**
         * @return current item to remove
         * @throws IllegalStateException if the {@code onNext} method has not
         *         yet been called, or the {@code itemToRemove} method has already
         *         been called after the last call to the {@code onNext}
         *         method
         */
        public T itemToRemove() {
            if (cur == null)
                throw new IllegalStateException();
            T res = cur;
            cur = null;
            return res;
        }
    }

}
