package org.apache.ignite.internal.processors.query.h2;

/**
 * Wrapper for a pooled object with capability to return the object to a pool.
 *
 * @param <T> Enclosed object type.
 */
public class ObjectPoolReusable<T extends AutoCloseable> {
    /** Object pool to recycle. */
    private final ObjectPool<T> pool;

    /** Detached object. */
    private T object;

    /**
     * @param pool Object pool.
     * @param object Detached object.
     */
    ObjectPoolReusable(ObjectPool<T> pool, T object) {
        this.pool = pool;
        this.object = object;
    }

    /**
     * @return Enclosed object.
     */
    public T object() {
        return object;
    }

    /**
     * Returns an object to a pool or closes it if the pool is already full.
     */
    public void recycle() {
        assert object != null  : "Already recycled";

        pool.recycle(object);

        object = null;
    }
}
