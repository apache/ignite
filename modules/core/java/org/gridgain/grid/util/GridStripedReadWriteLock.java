package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.locks.*;

/**
 * This is an utility class for 'splitting' locking of some
 * {@code int}- or {@code long}-keyed resources.
 *
 * Map {@code int} and {@code long} values to some number of locks,
 * and supply convenience methods to obtain and release these locks using key values.
 */
public class GridStripedReadWriteLock {
    /** Array of underlying locks. */
    @GridToStringExclude
    private final ReadWriteLock[] locks;

    /**
     * Creates new instance with the given concurrency level (number of locks).
     *
     * @param concurrencyLevel Concurrency level.
     */
    public GridStripedReadWriteLock(int concurrencyLevel) {
        locks = new ReadWriteLock[concurrencyLevel];

        for (int i = 0; i < concurrencyLevel; i++)
            locks[i] = new ReentrantReadWriteLock();
    }

    /**
     * Gets concurrency level.
     *
     * @return Concurrency level.
     */
    public int concurrencyLevel() {
        return locks.length;
    }

    /**
     * Gets all locks.
     *
     * @return All locks.
     */
    public ReadWriteLock[] getAllLocks() {
        return locks;
    }

    /**
     * Returns {@link Lock} object for the given key.
     * @param key Key.
     * @return Lock.
     */
    public ReadWriteLock getLock(int key) {
        return locks[U.safeAbs(key) % locks.length];
    }

    /**
     * Returns {@link Lock} object for the given key.
     * @param key Key.
     * @return Lock.
     */
    public ReadWriteLock getLock(long key) {
        return locks[U.safeAbs((int)(key % locks.length))];
    }

    /**
     * Returns lock for object.
     *
     * @param o Object.
     * @return Lock.
     */
    public ReadWriteLock getLock(@Nullable Object o) {
        return o == null ? locks[0] : getLock(o.hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridStripedReadWriteLock.class, this, "concurrency", locks.length);
    }
}
