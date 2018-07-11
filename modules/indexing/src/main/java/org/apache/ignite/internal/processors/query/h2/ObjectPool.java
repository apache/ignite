package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

public final class ObjectPool<E extends AutoCloseable> {
    public static class Reusable<T extends AutoCloseable> {
        private final ObjectPool<T> pool;
        private final T object;

        public Reusable(ObjectPool<T> pool, T object) {
            this.pool = pool;
            this.object = object;
        }

        public T object() {
            return object;
        }

        public void recycle() {
            if (!pool.bag.offer(this))
                U.closeQuiet(object);
        }
    }

    private final Supplier<E> connectionFactory;
    // TODO consider making it thread unsafe
    private final ArrayBlockingQueue<Reusable<E>> bag;

    public ObjectPool(Supplier<E> connectionFactory, int poolSize) {
        this.connectionFactory = connectionFactory;
        this.bag = new ArrayBlockingQueue<>(poolSize);
    }

    public Reusable<E> borrow() {
        Reusable<E> pooled = bag.poll();
        return pooled != null ? pooled : new Reusable<>(this, connectionFactory.get());
    }

}
