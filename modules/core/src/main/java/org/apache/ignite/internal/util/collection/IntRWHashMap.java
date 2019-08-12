/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.collection;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Read-write lock wrapper for {@link IntMap}.
 */
public class IntRWHashMap<V> implements IntMap<V> {
    /** RW Lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Map delegate. */
    private final IntHashMap<V> delegate;

    /** Default constructor. */
    public IntRWHashMap() {
        delegate = new IntHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public V get(int key) {
        lock.readLock().lock();
        try {
            return delegate.get(key);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public V put(int key, V val) {
        lock.writeLock().lock();
        try {
            return delegate.put(key, val);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public V remove(int key) {
        lock.writeLock().lock();
        try {
            return delegate.remove(key);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(int key, V val) {
        lock.writeLock().lock();
        try {
            return delegate.putIfAbsent(key, val);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <E extends Throwable> void forEach(EntryConsumer<V, E> act) throws E {
        lock.readLock().lock();
        try {
            delegate.forEach(act);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        lock.readLock().lock();
        try {
            return delegate.size();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(int key) {
        lock.readLock().lock();
        try {
            return delegate.containsKey(key);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V val) {
        lock.readLock().lock();
        try {
            return delegate.containsValue(val);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        lock.readLock().lock();
        try {
            return delegate.toString();
        }
        finally {
            lock.readLock().unlock();
        }
    }
}
