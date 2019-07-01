/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.h2.mvstore.DataUtils;

/**
 * A scan resistant cache that uses keys of type long. It is meant to cache
 * objects that are relatively costly to acquire, for example file content.
 * <p>
 * This implementation is multi-threading safe and supports concurrent access.
 * Null keys or null values are not allowed. The map fill factor is at most 75%.
 * <p>
 * Each entry is assigned a distinct memory size, and the cache will try to use
 * at most the specified amount of memory. The memory unit is not relevant,
 * however it is suggested to use bytes as the unit.
 * <p>
 * This class implements an approximation of the the LIRS replacement algorithm
 * invented by Xiaodong Zhang and Song Jiang as described in
 * http://www.cse.ohio-state.edu/~zhang/lirs-sigmetrics-02.html with a few
 * smaller changes: An additional queue for non-resident entries is used, to
 * prevent unbound memory usage. The maximum size of this queue is at most the
 * size of the rest of the stack. About 6.25% of the mapped entries are cold.
 * <p>
 * Internally, the cache is split into a number of segments, and each segment is
 * an individual LIRS cache.
 * <p>
 * Accessed entries are only moved to the top of the stack if at least a number
 * of other entries have been moved to the front (8 per segment by default).
 * Write access and moving entries to the top of the stack is synchronized per
 * segment.
 *
 * @author Thomas Mueller
 * @param <V> the value type
 */
public class CacheLongKeyLIRS<V> {

    /**
     * The maximum memory this cache should use.
     */
    private long maxMemory;

    private final Segment<V>[] segments;

    private final int segmentCount;
    private final int segmentShift;
    private final int segmentMask;
    private final int stackMoveDistance;
    private final int nonResidentQueueSize;

    /**
     * Create a new cache with the given memory size.
     *
     * @param config the configuration
     */
    @SuppressWarnings("unchecked")
    public CacheLongKeyLIRS(Config config) {
        setMaxMemory(config.maxMemory);
        this.nonResidentQueueSize = config.nonResidentQueueSize;
        DataUtils.checkArgument(
                Integer.bitCount(config.segmentCount) == 1,
                "The segment count must be a power of 2, is {0}", config.segmentCount);
        this.segmentCount = config.segmentCount;
        this.segmentMask = segmentCount - 1;
        this.stackMoveDistance = config.stackMoveDistance;
        segments = new Segment[segmentCount];
        clear();
        // use the high bits for the segment
        this.segmentShift = 32 - Integer.bitCount(segmentMask);
    }

    /**
     * Remove all entries.
     */
    public void clear() {
        long max = getMaxItemSize();
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment<>(
                    max, stackMoveDistance, 8, nonResidentQueueSize);
        }
    }

    /**
     * Determines max size of the data item size to fit into cache
     * @return data items size limit
     */
    public long getMaxItemSize() {
        return Math.max(1, maxMemory / segmentCount);
    }

    private Entry<V> find(long key) {
        int hash = getHash(key);
        return getSegment(hash).find(key, hash);
    }

    /**
     * Check whether there is a resident entry for the given key. This
     * method does not adjust the internal state of the cache.
     *
     * @param key the key (may not be null)
     * @return true if there is a resident entry
     */
    public boolean containsKey(long key) {
        int hash = getHash(key);
        return getSegment(hash).containsKey(key, hash);
    }

    /**
     * Get the value for the given key if the entry is cached. This method does
     * not modify the internal state.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V peek(long key) {
        Entry<V> e = find(key);
        return e == null ? null : e.value;
    }

    /**
     * Add an entry to the cache using the average memory size.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value) {
        return put(key, value, sizeOf(value));
    }

    /**
     * Add an entry to the cache. The entry may or may not exist in the
     * cache yet. This method will usually mark unknown entries as cold and
     * known entries as hot.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @param memory the memory used for the given entry
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value, int memory) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.put(key, hash, value, memory);
        }
    }

    private Segment<V> resizeIfNeeded(Segment<V> s, int segmentIndex) {
        int newLen = s.getNewMapLen();
        if (newLen == 0) {
            return s;
        }
        // another thread might have resized
        // (as we retrieved the segment before synchronizing on it)
        Segment<V> s2 = segments[segmentIndex];
        if (s == s2) {
            // no other thread resized, so we do
            s = new Segment<>(s, newLen);
            segments[segmentIndex] = s;
        }
        return s;
    }

    /**
     * Get the size of the given value. The default implementation returns 1.
     *
     * @param value the value
     * @return the size
     */
    @SuppressWarnings("unused")
    protected int sizeOf(V value) {
        return 1;
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be
     * removed.
     *
     * @param key the key (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V remove(long key) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.remove(key, hash);
        }
    }

    /**
     * Get the memory used for the given key.
     *
     * @param key the key (may not be null)
     * @return the memory, or 0 if there is no resident entry
     */
    public int getMemory(long key) {
        int hash = getHash(key);
        return getSegment(hash).getMemory(key, hash);
    }

    /**
     * Get the value for the given key if the entry is cached. This method
     * adjusts the internal state of the cache sometimes, to ensure commonly
     * used entries stay in the cache.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V get(long key) {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash);
    }

    private Segment<V> getSegment(int hash) {
        return segments[getSegmentIndex(hash)];
    }

    private int getSegmentIndex(int hash) {
        return (hash >>> segmentShift) & segmentMask;
    }

    /**
     * Get the hash code for the given key. The hash code is
     * further enhanced to spread the values more evenly.
     *
     * @param key the key
     * @return the hash code
     */
    static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        // a supplemental secondary hash function
        // to protect against hash codes that don't differ much
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    /**
     * Get the currently used memory.
     *
     * @return the used memory
     */
    public long getUsedMemory() {
        long x = 0;
        for (Segment<V> s : segments) {
            x += s.usedMemory;
        }
        return x;
    }

    /**
     * Set the maximum memory this cache should use. This will not
     * immediately cause entries to get removed however; it will only change
     * the limit. To resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger) in bytes
     */
    public void setMaxMemory(long maxMemory) {
        DataUtils.checkArgument(
                maxMemory > 0,
                "Max memory must be larger than 0, is {0}", maxMemory);
        this.maxMemory = maxMemory;
        if (segments != null) {
            long max = 1 + maxMemory / segments.length;
            for (Segment<V> s : segments) {
                s.setMaxMemory(max);
            }
        }
    }

    /**
     * Get the maximum memory to use.
     *
     * @return the maximum memory
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Get the entry set for all resident entries.
     *
     * @return the entry set
     */
    public synchronized Set<Map.Entry<Long, V>> entrySet() {
        HashMap<Long, V> map = new HashMap<>();
        for (long k : keySet()) {
            map.put(k,  find(k).value);
        }
        return map.entrySet();
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    public Set<Long> keySet() {
        HashSet<Long> set = new HashSet<>();
        for (Segment<V> s : segments) {
            set.addAll(s.keySet());
        }
        return set;
    }

    /**
     * Get the number of non-resident entries in the cache.
     *
     * @return the number of non-resident entries
     */
    public int sizeNonResident() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.queue2Size;
        }
        return x;
    }

    /**
     * Get the length of the internal map array.
     *
     * @return the size of the array
     */
    public int sizeMapArray() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.entries.length;
        }
        return x;
    }

    /**
     * Get the number of hot entries in the cache.
     *
     * @return the number of hot entries
     */
    public int sizeHot() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queueSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the number of cache hits.
     *
     * @return the cache hits
     */
    public long getHits() {
        long x = 0;
        for (Segment<V> s : segments) {
            x += s.hits;
        }
        return x;
    }

    /**
     * Get the number of cache misses.
     *
     * @return the cache misses
     */
    public long getMisses() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.misses;
        }
        return x;
    }

    /**
     * Get the number of resident entries.
     *
     * @return the number of entries
     */
    public int size() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the list of keys. This method allows to read the internal state of
     * the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    public List<Long> keys(boolean cold, boolean nonResident) {
        ArrayList<Long> keys = new ArrayList<>();
        for (Segment<V> s : segments) {
            keys.addAll(s.keys(cold, nonResident));
        }
        return keys;
    }

    /**
     * Get the values for all resident entries.
     *
     * @return the entry set
     */
    public List<V> values() {
        ArrayList<V> list = new ArrayList<>();
        for (long k : keySet()) {
            V value = find(k).value;
            if (value != null) {
                list.add(value);
            }
        }
        return list;
    }

    /**
     * Check whether the cache is empty.
     *
     * @return true if it is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Check whether the given value is stored.
     *
     * @param value the value
     * @return true if it is stored
     */
    public boolean containsValue(Object value) {
        return getMap().containsValue(value);
    }

    /**
     * Convert this cache to a map.
     *
     * @return the map
     */
    public Map<Long, V> getMap() {
        HashMap<Long, V> map = new HashMap<>();
        for (long k : keySet()) {
            V x = find(k).value;
            if (x != null) {
                map.put(k, x);
            }
        }
        return map;
    }

    /**
     * Add all elements of the map to this cache.
     *
     * @param m the map
     */
    public void putAll(Map<Long, ? extends V> m) {
        for (Map.Entry<Long, ? extends V> e : m.entrySet()) {
            // copy only non-null entries
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * A cache segment
     *
     * @param <V> the value type
     */
    private static class Segment<V> {

        /**
         * The number of (hot, cold, and non-resident) entries in the map.
         */
        int mapSize;

        /**
         * The size of the LIRS queue for resident cold entries.
         */
        int queueSize;

        /**
         * The size of the LIRS queue for non-resident cold entries.
         */
        int queue2Size;

        /**
         * The number of cache hits.
         */
        long hits;

        /**
         * The number of cache misses.
         */
        long misses;

        /**
         * The map array. The size is always a power of 2.
         */
        final Entry<V>[] entries;

        /**
         * The currently used memory.
         */
        long usedMemory;

        /**
         * How many other item are to be moved to the top of the stack before
         * the current item is moved.
         */
        private final int stackMoveDistance;

        /**
         * The maximum memory this cache should use in bytes.
         */
        private long maxMemory;

        /**
         * The bit mask that is applied to the key hash code to get the index in
         * the map array. The mask is the length of the array minus one.
         */
        private final int mask;

        /**
         * The number of entries in the non-resident queue, as a factor of the
         * number of entries in the map.
         */
        private final int nonResidentQueueSize;

        /**
         * The stack of recently referenced elements. This includes all hot
         * entries, and the recently referenced cold entries. Resident cold
         * entries that were not recently referenced, as well as non-resident
         * cold entries, are not in the stack.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private final Entry<V> stack;

        /**
         * The number of entries in the stack.
         */
        private int stackSize;

        /**
         * The queue of resident cold entries.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private final Entry<V> queue;

        /**
         * The queue of non-resident cold entries.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private final Entry<V> queue2;

        /**
         * The number of times any item was moved to the top of the stack.
         */
        private int stackMoveCounter;

        /**
         * Create a new cache segment.
         *
         * @param maxMemory the maximum memory to use
         * @param stackMoveDistance the number of other entries to be moved to
         *        the top of the stack before moving an entry to the top
         * @param len the number of hash table buckets (must be a power of 2)
         * @param nonResidentQueueSize the non-resident queue size factor
         */
        Segment(long maxMemory, int stackMoveDistance, int len,
                int nonResidentQueueSize) {
            setMaxMemory(maxMemory);
            this.stackMoveDistance = stackMoveDistance;
            this.nonResidentQueueSize = nonResidentQueueSize;

            // the bit mask has all bits set
            mask = len - 1;

            // initialize the stack and queue heads
            stack = new Entry<>();
            stack.stackPrev = stack.stackNext = stack;
            queue = new Entry<>();
            queue.queuePrev = queue.queueNext = queue;
            queue2 = new Entry<>();
            queue2.queuePrev = queue2.queueNext = queue2;

            @SuppressWarnings("unchecked")
            Entry<V>[] e = new Entry[len];
            entries = e;
        }

        /**
         * Create a new cache segment from an existing one.
         * The caller must synchronize on the old segment, to avoid
         * concurrent modifications.
         *
         * @param old the old segment
         * @param len the number of hash table buckets (must be a power of 2)
         */
        Segment(Segment<V> old, int len) {
            this(old.maxMemory, old.stackMoveDistance, len, old.nonResidentQueueSize);
            hits = old.hits;
            misses = old.misses;
            Entry<V> s = old.stack.stackPrev;
            while (s != old.stack) {
                Entry<V> e = copy(s);
                addToMap(e);
                addToStack(e);
                s = s.stackPrev;
            }
            s = old.queue.queuePrev;
            while (s != old.queue) {
                Entry<V> e = find(s.key, getHash(s.key));
                if (e == null) {
                    e = copy(s);
                    addToMap(e);
                }
                addToQueue(queue, e);
                s = s.queuePrev;
            }
            s = old.queue2.queuePrev;
            while (s != old.queue2) {
                Entry<V> e = find(s.key, getHash(s.key));
                if (e == null) {
                    e = copy(s);
                    addToMap(e);
                }
                addToQueue(queue2, e);
                s = s.queuePrev;
            }
        }

        /**
         * Calculate the new number of hash table buckets if the internal map
         * should be re-sized.
         *
         * @return 0 if no resizing is needed, or the new length
         */
        int getNewMapLen() {
            int len = mask + 1;
            if (len * 3 < mapSize * 4 && len < (1 << 28)) {
                // more than 75% usage
                return len * 2;
            } else if (len > 32 && len / 8 > mapSize) {
                // less than 12% usage
                return len / 2;
            }
            return 0;
        }

        private void addToMap(Entry<V> e) {
            int index = getHash(e.key) & mask;
            e.mapNext = entries[index];
            entries[index] = e;
            usedMemory += e.memory;
            mapSize++;
        }

        private static <V> Entry<V> copy(Entry<V> old) {
            Entry<V> e = new Entry<>();
            e.key = old.key;
            e.value = old.value;
            e.memory = old.memory;
            e.topMove = old.topMove;
            return e;
        }

        /**
         * Get the memory used for the given key.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the memory, or 0 if there is no resident entry
         */
        int getMemory(long key, int hash) {
            Entry<V> e = find(key, hash);
            return e == null ? 0 : e.memory;
        }

        /**
         * Get the value for the given key if the entry is cached. This method
         * adjusts the internal state of the cache sometimes, to ensure commonly
         * used entries stay in the cache.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the value, or null if there is no resident entry
         */
        V get(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null) {
                // the entry was not found
                misses++;
                return null;
            }
            V value = e.value;
            if (value == null) {
                // it was a non-resident entry
                misses++;
                return null;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 ||
                            stackMoveCounter - e.topMove > stackMoveDistance) {
                        access(key, hash);
                    }
                }
            } else {
                access(key, hash);
            }
            hits++;
            return value;
        }

        /**
         * Access an item, moving the entry to the top of the stack or front of
         * the queue if found.
         *
         * @param key the key
         */
        private synchronized void access(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null || e.value == null) {
                return;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 ||
                            stackMoveCounter - e.topMove > stackMoveDistance) {
                        // move a hot entry to the top of the stack
                        // unless it is already there
                        boolean wasEnd = e == stack.stackPrev;
                        removeFromStack(e);
                        if (wasEnd) {
                            // if moving the last entry, the last entry
                            // could now be cold, which is not allowed
                            pruneStack();
                        }
                        addToStack(e);
                    }
                }
            } else {
                removeFromQueue(e);
                if (e.stackNext != null) {
                    // resident cold entries become hot
                    // if they are on the stack
                    removeFromStack(e);
                    // which means a hot entry needs to become cold
                    // (this entry is cold, that means there is at least one
                    // more entry in the stack, which must be hot)
                    convertOldestHotToCold();
                } else {
                    // cold entries that are not on the stack
                    // move to the front of the queue
                    addToQueue(queue, e);
                }
                // in any case, the cold entry is moved to the top of the stack
                addToStack(e);
            }
        }

        /**
         * Add an entry to the cache. The entry may or may not exist in the
         * cache yet. This method will usually mark unknown entries as cold and
         * known entries as hot.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @param value the value (may not be null)
         * @param memory the memory used for the given entry
         * @return the old value, or null if there was no resident entry
         */
        synchronized V put(long key, int hash, V value, int memory) {
            if (value == null) {
                throw DataUtils.newIllegalArgumentException(
                        "The value may not be null");
            }
            V old;
            Entry<V> e = find(key, hash);
            boolean existed;
            if (e == null) {
                existed = false;
                old = null;
            } else {
                existed = true;
                old = e.value;
                remove(key, hash);
            }
            if (memory > maxMemory) {
                // the new entry is too big to fit
                return old;
            }
            e = new Entry<>();
            e.key = key;
            e.value = value;
            e.memory = memory;
            int index = hash & mask;
            e.mapNext = entries[index];
            entries[index] = e;
            usedMemory += memory;
            if (usedMemory > maxMemory) {
                // old entries needs to be removed
                evict();
                // if the cache is full, the new entry is
                // cold if possible
                if (stackSize > 0) {
                    // the new cold entry is at the top of the queue
                    addToQueue(queue, e);
                }
            }
            mapSize++;
            // added entries are always added to the stack
            addToStack(e);
            if (existed) {
                // if it was there before (even non-resident), it becomes hot
                access(key, hash);
            }
            return old;
        }

        /**
         * Remove an entry. Both resident and non-resident entries can be
         * removed.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the old value, or null if there was no resident entry
         */
        synchronized V remove(long key, int hash) {
            int index = hash & mask;
            Entry<V> e = entries[index];
            if (e == null) {
                return null;
            }
            V old;
            if (e.key == key) {
                old = e.value;
                entries[index] = e.mapNext;
            } else {
                Entry<V> last;
                do {
                    last = e;
                    e = e.mapNext;
                    if (e == null) {
                        return null;
                    }
                } while (e.key != key);
                old = e.value;
                last.mapNext = e.mapNext;
            }
            mapSize--;
            usedMemory -= e.memory;
            if (e.stackNext != null) {
                removeFromStack(e);
            }
            if (e.isHot()) {
                // when removing a hot entry, the newest cold entry gets hot,
                // so the number of hot entries does not change
                e = queue.queueNext;
                if (e != queue) {
                    removeFromQueue(e);
                    if (e.stackNext == null) {
                        addToStackBottom(e);
                    }
                }
            } else {
                removeFromQueue(e);
            }
            pruneStack();
            return old;
        }

        /**
         * Evict cold entries (resident and non-resident) until the memory limit
         * is reached. The new entry is added as a cold entry, except if it is
         * the only entry.
         */
        private void evict() {
            do {
                evictBlock();
            } while (usedMemory > maxMemory);
        }

        private void evictBlock() {
            // ensure there are not too many hot entries: right shift of 5 is
            // division by 32, that means if there are only 1/32 (3.125%) or
            // less cold entries, a hot entry needs to become cold
            while (queueSize <= (mapSize >>> 5) && stackSize > 0) {
                convertOldestHotToCold();
            }
            // the oldest resident cold entries become non-resident
            while (usedMemory > maxMemory && queueSize > 0) {
                Entry<V> e = queue.queuePrev;
                usedMemory -= e.memory;
                removeFromQueue(e);
                e.value = null;
                e.memory = 0;
                addToQueue(queue2, e);
                // the size of the non-resident-cold entries needs to be limited
                int maxQueue2Size = nonResidentQueueSize * (mapSize - queue2Size);
                if (maxQueue2Size >= 0) {
                    while (queue2Size > maxQueue2Size) {
                        e = queue2.queuePrev;
                        int hash = getHash(e.key);
                        remove(e.key, hash);
                    }
                }
            }
        }

        private void convertOldestHotToCold() {
            // the last entry of the stack is known to be hot
            Entry<V> last = stack.stackPrev;
            if (last == stack) {
                // never remove the stack head itself (this would mean the
                // internal structure of the cache is corrupt)
                throw new IllegalStateException();
            }
            // remove from stack - which is done anyway in the stack pruning,
            // but we can do it here as well
            removeFromStack(last);
            // adding an entry to the queue will make it cold
            addToQueue(queue, last);
            pruneStack();
        }

        /**
         * Ensure the last entry of the stack is cold.
         */
        private void pruneStack() {
            while (true) {
                Entry<V> last = stack.stackPrev;
                // must stop at a hot entry or the stack head,
                // but the stack head itself is also hot, so we
                // don't have to test it
                if (last.isHot()) {
                    break;
                }
                // the cold entry is still in the queue
                removeFromStack(last);
            }
        }

        /**
         * Try to find an entry in the map.
         *
         * @param key the key
         * @param hash the hash
         * @return the entry (might be a non-resident)
         */
        Entry<V> find(long key, int hash) {
            int index = hash & mask;
            Entry<V> e = entries[index];
            while (e != null && e.key != key) {
                e = e.mapNext;
            }
            return e;
        }

        private void addToStack(Entry<V> e) {
            e.stackPrev = stack;
            e.stackNext = stack.stackNext;
            e.stackNext.stackPrev = e;
            stack.stackNext = e;
            stackSize++;
            e.topMove = stackMoveCounter++;
        }

        private void addToStackBottom(Entry<V> e) {
            e.stackNext = stack;
            e.stackPrev = stack.stackPrev;
            e.stackPrev.stackNext = e;
            stack.stackPrev = e;
            stackSize++;
        }

        /**
         * Remove the entry from the stack. The head itself must not be removed.
         *
         * @param e the entry
         */
        private void removeFromStack(Entry<V> e) {
            e.stackPrev.stackNext = e.stackNext;
            e.stackNext.stackPrev = e.stackPrev;
            e.stackPrev = e.stackNext = null;
            stackSize--;
        }

        private void addToQueue(Entry<V> q, Entry<V> e) {
            e.queuePrev = q;
            e.queueNext = q.queueNext;
            e.queueNext.queuePrev = e;
            q.queueNext = e;
            if (e.value != null) {
                queueSize++;
            } else {
                queue2Size++;
            }
        }

        private void removeFromQueue(Entry<V> e) {
            e.queuePrev.queueNext = e.queueNext;
            e.queueNext.queuePrev = e.queuePrev;
            e.queuePrev = e.queueNext = null;
            if (e.value != null) {
                queueSize--;
            } else {
                queue2Size--;
            }
        }

        /**
         * Get the list of keys. This method allows to read the internal state
         * of the cache.
         *
         * @param cold if true, only keys for the cold entries are returned
         * @param nonResident true for non-resident entries
         * @return the key list
         */
        synchronized List<Long> keys(boolean cold, boolean nonResident) {
            ArrayList<Long> keys = new ArrayList<>();
            if (cold) {
                Entry<V> start = nonResident ? queue2 : queue;
                for (Entry<V> e = start.queueNext; e != start;
                        e = e.queueNext) {
                    keys.add(e.key);
                }
            } else {
                for (Entry<V> e = stack.stackNext; e != stack;
                        e = e.stackNext) {
                    keys.add(e.key);
                }
            }
            return keys;
        }

        /**
         * Check whether there is a resident entry for the given key. This
         * method does not adjust the internal state of the cache.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return true if there is a resident entry
         */
        boolean containsKey(long key, int hash) {
            Entry<V> e = find(key, hash);
            return e != null && e.value != null;
        }

        /**
         * Get the set of keys for resident entries.
         *
         * @return the set of keys
         */
        synchronized Set<Long> keySet() {
            HashSet<Long> set = new HashSet<>();
            for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
                set.add(e.key);
            }
            for (Entry<V> e = queue.queueNext; e != queue; e = e.queueNext) {
                set.add(e.key);
            }
            return set;
        }

        /**
         * Set the maximum memory this cache should use. This will not
         * immediately cause entries to get removed however; it will only change
         * the limit. To resize the internal array, call the clear method.
         *
         * @param maxMemory the maximum size (1 or larger) in bytes
         */
        void setMaxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
        }

    }

    /**
     * A cache entry. Each entry is either hot (low inter-reference recency;
     * LIR), cold (high inter-reference recency; HIR), or non-resident-cold. Hot
     * entries are in the stack only. Cold entries are in the queue, and may be
     * in the stack. Non-resident-cold entries have their value set to null and
     * are in the stack and in the non-resident queue.
     *
     * @param <V> the value type
     */
    static class Entry<V> {

        /**
         * The key.
         */
        long key;

        /**
         * The value. Set to null for non-resident-cold entries.
         */
        V value;

        /**
         * The estimated memory used.
         */
        int memory;

        /**
         * When the item was last moved to the top of the stack.
         */
        int topMove;

        /**
         * The next entry in the stack.
         */
        Entry<V> stackNext;

        /**
         * The previous entry in the stack.
         */
        Entry<V> stackPrev;

        /**
         * The next entry in the queue (either the resident queue or the
         * non-resident queue).
         */
        Entry<V> queueNext;

        /**
         * The previous entry in the queue.
         */
        Entry<V> queuePrev;

        /**
         * The next entry in the map (the chained entry).
         */
        Entry<V> mapNext;

        /**
         * Whether this entry is hot. Cold entries are in one of the two queues.
         *
         * @return whether the entry is hot
         */
        boolean isHot() {
            return queueNext == null;
        }

    }

    /**
     * The cache configuration.
     */
    public static class Config {

        /**
         *  The maximum memory to use (1 or larger).
         */
        public long maxMemory = 1;

        /**
         * The number of cache segments (must be a power of 2).
         */
        public int segmentCount = 16;

        /**
         * How many other item are to be moved to the top of the stack before
         * the current item is moved.
         */
        public int stackMoveDistance = 32;

        /**
         * The number of entries in the non-resident queue, as a factor of the
         * number of all other entries in the map.
         */
        public final int nonResidentQueueSize = 3;

    }

}
