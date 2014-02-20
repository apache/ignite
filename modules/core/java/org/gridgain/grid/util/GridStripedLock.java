package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.locks.*;

/**
 * This is an utility class for 'splitting' locking of some
 * {@code int}- or {@code long}-keyed resources.
 *
 * Map {@code int} and {@code long} values to some number of locks,
 * and supply convenience methods to obtain and release these locks using key values.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridStripedLock {
    /** Array of underlying locks. */
    private final Lock[] locks;

    /**
     * Creates new instance with the given concurrency level (number of locks).
     *
     * @param concurrencyLevel Concurrency level.
     */
    public GridStripedLock(int concurrencyLevel) {
        locks = new Lock[concurrencyLevel];

        for (int i = 0; i < concurrencyLevel; i++)
            locks[i] = new ReentrantLock();
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
     * Returns {@link Lock} object for the given key.
     * @param key Key.
     * @return Lock.
     */
    public Lock getLock(int key) {
        return locks[U.safeAbs(key) % locks.length];
    }

    /**
     * Returns {@link Lock} object for the given key.
     * @param key Key.
     * @return Lock.
     */
    public Lock getLock(long key) {
        return locks[U.safeAbs((int)(key % locks.length))];
    }

    /**
     * Returns lock for object.
     *
     * @param o Object.
     * @return Lock.
     */
    public Lock getLock(@Nullable Object o) {
        return o == null ? locks[0] : getLock(o.hashCode());
    }

    /**
     * Locks given key.
     *
     * @param key Key.
     */
    public void lock(int key) {
        getLock(key).lock();
    }

    /**
     * Unlocks given key.
     *
     * @param key Key.
     */
    public void unlock(int key) {
        getLock(key).unlock();
    }

    /**
     * Locks given key.
     *
     * @param key Key.
     */
    public void lock(long key) {
        getLock(key).lock();
    }

    /**
     * Unlocks given key.
     *
     * @param key Key.
     */
    public void unlock(long key) {
        getLock(key).unlock();
    }

    /**
     * Locks an object.
     *
     * @param o Object.
     */
    public void lock(@Nullable Object o) {
        getLock(o).lock();
    }

    /**
     * Unlocks an object.
     *
     * @param o Object.
     */
    public void unlock(@Nullable Object o) {
        getLock(o).unlock();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridStripedLock.class, this, "concurrency", locks.length);
    }
}
