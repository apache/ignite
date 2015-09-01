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

package org.apache.ignite.internal.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * This is an utility class for 'splitting' locking of some
 * {@code int}- or {@code long}-keyed resources.
 *
 * Map {@code int} and {@code long} values to some number of locks,
 * and supply convenience methods to obtain and release these locks using key values.
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