package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

/**
 * Not thread-safe pool for managing limited number objects for further reuse.
 *
 * @param <E> pooled objects type
 */
public final class ObjectPool<E extends AutoCloseable> {
    /**
     * Wrapper for a pooled object with capability to return the object to a pool.
     *
     * @param <T> enclosed object type
     */
    public static class Reusable<T extends AutoCloseable> {
        private final ObjectPool<T> pool;
        private final T object;

        private Reusable(ObjectPool<T> pool, T object) {
            this.pool = pool;
            this.object = object;
        }

        /**
         * @return enclosed object
         */
        public T object() {
            return object;
        }

        /**
         * Returns an object to a pool or closes it if the pool is already full.
         */
        public void recycle() {
            if (!pool.bag.offer(this))
                U.closeQuiet(object);
        }
    }

    private final Supplier<E> objectFactory;
    // TODO consider making it thread unsafe
    private final ArrayBlockingQueue<Reusable<E>> bag;

    /**
     * @param objectFactory factory used for new objects creation
     * @param poolSize number of objects which pool can contain
     */
    public ObjectPool(Supplier<E> objectFactory, int poolSize) {
        this.objectFactory = objectFactory;
        this.bag = new ArrayBlockingQueue<>(poolSize);
    }

    /**
     * Picks an object from the pool if one is present or creates new one otherwise.
     * Returns an object wrapper which could be returned to the pool.
     *
     * @return reusable object wrapper
     */
    public Reusable<E> borrow() {
        Reusable<E> pooled = bag.poll();
        return pooled != null ? pooled : new Reusable<>(this, objectFactory.get());
    }
}
