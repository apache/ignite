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

package org.apache.ignite.internal.util.collection;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The simple map for primitive types base on Robin-hood hashing with backward shift.
 */
public class IntHashMap<V> implements IntMap<V> {
    /** Array load percentage before resize. */
    private static final float SCALE_LOAD_FACTOR = 0.9F;

    /** Array load percentage before decrise size. */
    private static final float COMPACT_LOAD_FACTOR = 0.5F;

    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 8;

    /** Maximum capacity. */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /** RW Lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Entries. */
    private Entry<V>[] entries;

    /** Count of elements into Map. */
    private int size;

    /** Inner entry structure. */
    private static class Entry<V> {
        /** Key. */
        private final int key;

        /** Value. */
        private final V val;

        /** Default constructor. */
        Entry(int key, V val) {
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "{key=" + key + ", val=" + val + '}';
        }
    }

    /** Default constructor. */
    public IntHashMap() {
        entries = (Entry<V>[])new Entry[INITIAL_CAPACITY];
    }

    /** {@inheritDoc} */
    @Override public V get(int key) {
        lock.readLock().lock();
        try {
            int idx = find(key);

            return idx < 0 ? null : entries[idx].val;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public V put(int key, V val) {
        lock.writeLock().lock();
        try {
            return put0(new Entry<>(key, val));
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public V remove(int key) {
        lock.writeLock().lock();
        try {
            if (entries.length != INITIAL_CAPACITY && (int)(COMPACT_LOAD_FACTOR * (entries.length >> 1)) > size)
                resize(false);

            int idx = find(key);

            if (idx < 0)
                return null;

            size--;

            V tmp = entries[idx].val;

            // Backward shift
            for (int i = 0; i < entries.length; i++) {
                int curIdx = (idx + i) & (entries.length - 1);
                int nextIdx = (idx + i + 1) & (entries.length - 1);

                Entry<V> nextEntry = entries[nextIdx];

                if (nextEntry == null || distance(nextIdx, nextEntry.key) == 0) {
                    entries[curIdx] = null;

                    return tmp;
                }

                entries[curIdx] = entries[nextIdx];
            }

            throw new IllegalStateException("Unreachable state exception. Backward shift has a problem. " +
                "Removing key: " + key + " map state: " + toString());
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(int key, V val) {
        lock.writeLock().lock();
        try {
            int idx = find(key);

            if (idx < 0) {
                put(key, val);

                return null;
            }
            else
                return entries[idx].val;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <E extends Throwable> void forEach(EntryConsumer<V, E> act) throws E {
        lock.readLock().lock();
        try {
            for (Entry<V> entry : entries)
                if (entry != null)
                    act.accept(entry.key, entry.val);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(int key) {
        return find(key) >= 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V val) {
        return Arrays.stream(entries)
            .filter(Objects::nonNull)
            .anyMatch(entry -> Objects.equals(val, entry.val));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String strEntries = Arrays.stream(entries)
            .filter(Objects::nonNull)
            .map(Entry::toString)
            .collect(Collectors.joining(","));

        return "IntHashMap{size=" + size + ", entries=[" + strEntries + "]}";
    }

    /**
     * Returns size of shift from ideal position computing via index(key).
     *
     * @param curIdx Current index.
     * @param key Target key.
     */
    protected int distance(int curIdx, int key) {
        int keyIdx = index(key);

        return curIdx >= keyIdx ? curIdx - keyIdx : entries.length - keyIdx + curIdx;
    }

    /**
     * Position in entites array.
     *
     * @param key Targert key.
     */
    protected int index(int key) {
        return (entries.length - 1) & (key ^ (key >>> 16));
    }

    /**
     * Does insert of entry.
     */
    private V put0(Entry<V> entry) {
        if (size >= (int)(entries.length * SCALE_LOAD_FACTOR))
            resize(true);

        Entry<V> savedEntry = entry;

        int startKey = savedEntry.key;

        for (int i = 0; i < entries.length; i++) {
            int idx = (index(startKey) + i) & (entries.length - 1);

            Entry<V> curEntry = entries[idx];

            if (curEntry == null) {
                entries[idx] = savedEntry;

                size++;

                return null;
            }
            else if (curEntry.key == savedEntry.key) {
                entries[idx] = savedEntry;

                return curEntry.val;
            }

            int curDist = distance(idx, curEntry.key);
            int savedDist = distance(idx, savedEntry.key);

            if (curDist < savedDist) {
                entries[idx] = savedEntry;

                savedEntry = curEntry;
            }
        }

        throw new IllegalStateException("Unreachable state exception. Insertion position not found. " +
            "Entry: " + entry + " map state: " + toString());
    }

    /**
     * @param key Key.
     * @return position of element or -1 if absent.
     */
    private int find(int key) {
        int idx = index(key);

        for (int i = 0; i < entries.length; i++) {
            int curIdx = (idx + i) & (entries.length - 1);

            Entry<V> entry = entries[curIdx];

            if (entry == null)
                return -1;
            else if (entry.key == key)
                return curIdx;

            int keyDist = distance(curIdx, key);
            int entryDist = distance(curIdx, entry.key);

            if (keyDist > entryDist)
                return -1;
        }

        return -1;
    }

    /**
     * Does resize of entities array.
     */
    private void resize(boolean increase) {
        if (MAXIMUM_CAPACITY == entries.length)
            throw new IllegalStateException("Maximum capacity: " + MAXIMUM_CAPACITY + " is reached.");

        Entry<V>[] oldEntries = entries;

        int newCap = increase ? entries.length << 1 : entries.length >> 1;

        entries = (Entry<V>[])new Entry[newCap];

        size = 0;

        for (Entry<V> entry : oldEntries)
            if (entry != null)
                put0(entry);
    }
}
