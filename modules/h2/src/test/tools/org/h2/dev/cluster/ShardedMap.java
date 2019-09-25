/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.cluster;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;

/**
 * A sharded map. It is typically split into multiple sub-maps that don't have
 * overlapping keys.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ShardedMap<K, V> extends AbstractMap<K, V> {

    private final DataType keyType;

    /**
     * The shards. Each shard has a minimum and a maximum key (null for no
     * limit). Key ranges are ascending but can overlap, in which case entries
     * may be stored in multiple maps. If that is the case, for read operations,
     * the entry in the first map is used, and for write operations, the changes
     * are applied to all maps within the range.
     */
    private Shard<K, V>[] shards;

    public ShardedMap() {
        this(new ObjectDataType());
    }

    @SuppressWarnings("unchecked")
    public ShardedMap(DataType keyType) {
        this.keyType = keyType;
        shards = new Shard[0];
    }

    /**
     * Get the size of the map.
     *
     * @param map the map
     * @return the size
     */
    static long getSize(Map<?, ?> map) {
        if (map instanceof LargeMap) {
            return ((LargeMap) map).sizeAsLong();
        }
        return map.size();
    }

    /**
     * Add the given shard.
     *
     * @param map the map
     * @param min the lowest key, or null if no limit
     * @param max the highest key, or null if no limit
     */
    public void addMap(Map<K, V> map, K min, K max) {
        if (min != null && max != null && keyType.compare(min, max) > 0) {
            DataUtils.newIllegalArgumentException("Invalid range: {0} .. {1}", min, max);
        }
        int len = shards.length + 1;
        Shard<K, V>[] newShards = Arrays.copyOf(shards, len);
        Shard<K, V> newShard = new Shard<>();
        newShard.map = map;
        newShard.minIncluding = min;
        newShard.maxExcluding = max;
        newShards[len - 1] = newShard;
        shards = newShards;
    }

    private boolean isInRange(K key, Shard<K, V> shard) {
        if (shard.minIncluding != null) {
            if (keyType.compare(key, shard.minIncluding) < 0) {
                return false;
            }
        }
        if (shard.maxExcluding != null) {
            if (keyType.compare(key, shard.maxExcluding) >= 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * The size of the map.
     *
     * @return the size
     */
    public long sizeAsLong() {
        Shard<K, V>[] copy = shards;
        for (Shard<K, V> s : copy) {
            if (s.minIncluding == null && s.maxExcluding == null) {
                return getSize(s.map);
            }
        }
        if (isSimpleSplit(copy)) {
            long size = 0;
            for (Shard<K, V> s : copy) {
                size += getSize(s.map);
            }
            return size;
        }
        return -1;
    }

    private boolean isSimpleSplit(Shard<K, V>[] shards) {
        K last = null;
        for (int i = 0; i < shards.length; i++) {
            Shard<K, V> s = shards[i];
            if (last == null) {
                if (s.minIncluding != null) {
                    return false;
                }
            } else if (keyType.compare(last, s.minIncluding) != 0) {
                return false;
            }
            if (s.maxExcluding == null) {
                return i == shards.length - 1;
            }
            last = s.maxExcluding;
        }
        return last == null;
    }

    @Override
    public V put(K key, V value) {
        V result = null;
        Shard<K, V>[] copy = shards;
        for (Shard<K, V> s : copy) {
            if (isInRange(key, s)) {
                V r = s.map.put(key, value);
                if (result == null) {
                    result = r;
                }
            }
        }
        return result;
    }

    @Override
    public V get(Object key) {
        @SuppressWarnings("unchecked")
        K k = (K) key;
        Shard<K, V>[] copy = shards;
        for (Shard<K, V> s : copy) {
            if (isInRange(k, s)) {
                return s.map.get(k);
            }
        }
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Shard<K, V>[] copy = shards;
        for (Shard<K, V> s : copy) {
            if (s.minIncluding == null && s.maxExcluding == null) {
                return s.map.entrySet();
            }
        }
        if (isSimpleSplit(copy)) {
            return new CombinedSet<>(size(), copy);
        }
        return null;
    }

    /**
     * A subset of a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Shard<K, V> {

        /**
         * The lowest key, or null if no limit.
         */
        K minIncluding;

        /**
         * A key higher than the highest key, or null if no limit.
         */
        K maxExcluding;

        /**
         * The backing map.
         */
        Map<K, V> map;

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            if (minIncluding != null) {
                buff.append('[').append(minIncluding);
            }
            buff.append("..");
            if (maxExcluding != null) {
                buff.append(maxExcluding).append(')');
            }
            return buff.toString();
        }

    }

    /**
     * A combination of multiple sets.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    private static class CombinedSet<K, V> extends AbstractSet<Entry<K, V>> {

        final int size;
        final Shard<K, V>[] shards;

        CombinedSet(int size, Shard<K, V>[] shards) {
            this.size = size;
            this.shards = shards;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new Iterator<Entry<K, V>>() {

                boolean init;
                Entry<K, V> current;
                Iterator<Entry<K, V>> currentIterator;
                int shardIndex;

                private void fetchNext() {
                    while (currentIterator == null || !currentIterator.hasNext()) {
                        if (shardIndex >= shards.length) {
                            current = null;
                            return;
                        }
                        currentIterator = shards[shardIndex++].map.entrySet().iterator();
                    }
                    current = currentIterator.next();
                }

                @Override
                public boolean hasNext() {
                    if (!init) {
                        fetchNext();
                        init = true;
                    }
                    return current != null;
                }

                @Override
                public Entry<K, V> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    Entry<K, V> e = current;
                    fetchNext();
                    return e;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
        }

        @Override
        public int size() {
            return size;
        }

    }

    /**
     * A large map.
     */
    public interface LargeMap {

        /**
         * The size of the map.
         *
         * @return the size
         */
        long sizeAsLong();
    }

    /**
     * A map that can efficiently return the index of a key, and the key at a
     * given index.
     */
    public interface CountedMap<K, V> {

        /**
         * Get the key at the given index.
         *
         * @param index the index
         * @return the key
         */
        K getKey(long index);

        /**
         * Get the index of the given key in the map.
         * <p>
         * If the key was found, the returned value is the index in the key
         * array. If not found, the returned value is negative, where -1 means
         * the provided key is smaller than any keys. See also
         * Arrays.binarySearch.
         *
         * @param key the key
         * @return the index
         */
        long getKeyIndex(K key);
    }

}
