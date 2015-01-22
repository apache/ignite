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

package org.apache.ignite.streamer.index;

import com.romix.scala.*;
import com.romix.scala.collection.concurrent.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;
import org.pcollections.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.streamer.index.StreamerIndexPolicy.*;

/**
 * Convenient {@link StreamerIndexProvider} adapter implementing base configuration methods.
 */
public abstract class StreamerIndexProviderAdapter<E, K, V> implements StreamerIndexProvider<E, K, V> {
    /** */
    protected final IgniteClosure<StreamerIndexEntry<E, K, V>, V> entryToVal =
        new C1<StreamerIndexEntry<E, K, V>, V>() {
            @Override public V apply(StreamerIndexEntry<E, K, V> e) {
                return e.value();
            }
        };

    /** */
    protected final IgniteClosure<StreamerIndexEntry<E, K, V>, K> entryToKey =
        new C1<StreamerIndexEntry<E, K, V>, K>() {
            @Override public K apply(StreamerIndexEntry<E, K, V> e) {
                return e.key();
            }
        };

    /** Keys currently being updated. */
    private final ConcurrentMap<K, StreamerIndexUpdateSync> locks = new ConcurrentHashMap8<>();

    /** Index name. */
    private String name;

    /** Index policy. */
    private StreamerIndexPolicy plc = EVENT_TRACKING_OFF;

    /** Index updater. */
    private StreamerIndexUpdater<E, K, V> updater;

    /** */
    private final LongAdder evtsCnt = new LongAdder();

    /** Read write lock. */
    private final GridSpinReadWriteLock rwLock = new GridSpinReadWriteLock();

    /** */
    private boolean unique;

    /** */
    private final ThreadLocal<K> threadLocKey = new ThreadLocal<>();

    /** */
    private final ConcurrentMap<IndexKey<V>, StreamerIndexUpdateSync> idxLocks = new ConcurrentHashMap8<>();

    /** */
    private boolean keyCheck = true;

    /**
     * Sets index name.
     *
     * @param name Index name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /**
     * Sets index policy.
     *
     * @param plc Policy.
     */
    public void setPolicy(StreamerIndexPolicy plc) {
        this.plc = plc;
    }

    /** {@inheritDoc} */
    @Override public StreamerIndexPolicy getPolicy() {
        return plc;
    }

    /**
     * Sets unique flag.
     *
     * @param unique {@code True} for unique index.
     */
    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    /** {@inheritDoc} */
    @Override public boolean isUnique() {
        return unique;
    }

    /**
     * Sets index updater.
     *
     * @param updater Updater.
     */
    public void setUpdater(StreamerIndexUpdater<E, K, V> updater) {
        this.updater = updater;
    }

    /**
     * Gets index updater.
     *
     * @return Updater.
     */
    public StreamerIndexUpdater<E, K, V> getUpdater() {
        return updater;
    }

    /** {@inheritDoc} */
    @Override public void dispose() {
        // No-op.
    }

    /**
     * Add event to the index.
     *
     * @param sync Sync.
     * @param evt Event.
     */
    @Override public void add(StreamerIndexUpdateSync sync, E evt) throws IgniteCheckedException {
        assert evt != null;

        if (threadLocKey.get() != null)
            throw new IllegalStateException("Previous operation has not been finished: " + threadLocKey.get());

        K key = updater.indexKey(evt);

        if (key == null)
            return; // Ignore event.

        validateIndexKey(key);

        readLock();

        threadLocKey.set(key);

        lockKey(key, sync);

        add(evt, key, sync);
    }

    /**
     * Remove event from the index.
     *
     * @param sync Sync.
     * @param evt Event.
     */
    @Override public void remove(StreamerIndexUpdateSync sync, E evt) throws IgniteCheckedException {
        assert evt != null;

        if (threadLocKey.get() != null)
            throw new IllegalStateException("Previous operation has not been finished: " + threadLocKey.get());

        K key = updater.indexKey(evt);

        assert key != null;

        validateIndexKey(key);

        readLock();

        threadLocKey.set(key);

        lockKey(key, sync);

        remove(evt, key, sync);
    }

    /** {@inheritDoc} */
    @Override public void endUpdate(StreamerIndexUpdateSync sync, E evt, boolean rollback, boolean rmv) {
        K key = threadLocKey.get();

        if (key == null)
            return;

        if (!rollback) {
            if (rmv)
                evtsCnt.decrement();
            else
                evtsCnt.increment();
        }

        threadLocKey.remove();

        endUpdate0(sync, evt, key, rollback);

        unlockKey(key, sync);

        readUnlock();
    }

    /**
     * @param sync Sync.
     * @param evt Event.
     * @param key Key.
     * @param rollback Rollback flag.
     */
    protected abstract void endUpdate0(StreamerIndexUpdateSync sync, E evt, K key, boolean rollback);

    /** {@inheritDoc} */
    @Override public void reset() {
        writeLock();

        try {
            reset0();
        }
        finally {
            writeUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public StreamerIndex<E, K, V> index() {
        writeLock();

        try {
            return index0();
        }
        finally {
            writeUnlock();
        }
    }

    /**
     * Called on reset.
     */
    protected abstract void reset0();

    /**
     * @return Index
     */
    protected abstract StreamerIndex<E, K, V> index0();

    /**
     *
     */
    protected void readLock() {
        rwLock.readLock();
    }

    /**
     *
     */
    protected void readUnlock() {
        rwLock.readUnlock();
    }

    /**
     *
     */
    protected void writeLock() {
        rwLock.writeLock();
    }

    /**
     *
     */
    protected void writeUnlock() {
        rwLock.writeUnlock();
    }

    /**
     * @return Events count,
     */
    protected int eventsCount() {
        return evtsCnt.intValue();
    }

    /**
     * Add event to the index.
     *
     * @param evt Event.
     * @param key key.
     * @param sync Sync.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void add(E evt, K key, StreamerIndexUpdateSync sync) throws IgniteCheckedException;

    /**
     * Remove event from the index.
     *
     * @param evt Event.
     * @param key Key.
     * @param sync Sync.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void remove(E evt, K key, StreamerIndexUpdateSync sync) throws IgniteCheckedException;

    /**
     * Lock updates on particular key.
     *
     * @param key Key.
     * @param sync Sync.
     * @throws IgniteCheckedException If failed.
     */
    private void lockKey(K key, StreamerIndexUpdateSync sync) throws IgniteCheckedException {
        assert key != null;
        assert sync != null;

        while (true) {
            StreamerIndexUpdateSync old = locks.putIfAbsent(key, sync);

            if (old != null) {
                try {
                    old.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException("Failed to lock on key (thread has been interrupted): " + key, e);
                }

                // No point to replace or remove sync here.
                // Owner will first remove it, then will finish the sync.
            }
            else
                break;
        }
    }

    /**
     * Unlock updates on particular key.
     *
     * @param key Key.
     * @param sync Sync.
     */
    private void unlockKey(K key, StreamerIndexUpdateSync sync) {
        assert key != null;

        locks.remove(key, sync);
    }

    /**
     * Lock updates on particular key.
     *
     * @param key Key.
     * @param sync Sync.
     * @throws IgniteCheckedException If failed.
     */
    protected void lockIndexKey(IndexKey<V> key, StreamerIndexUpdateSync sync) throws IgniteCheckedException {
        assert key != null;
        assert sync != null;
        assert isUnique();

        while (true) {
            StreamerIndexUpdateSync old = idxLocks.putIfAbsent(key, sync);

            if (old != null) {
                try {
                    old.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException("Failed to lock on key (thread has been interrupted): " + key, e);
                }

                // No point to replace or remove sync here.
                // Owner will first remove it, then will finish the sync.
            }
            else
                break;
        }
    }

    /**
     * Unlock updates on particular key.
     *
     * @param key Key.
     * @param sync Sync.
     */
    protected void unlockIndexKey(IndexKey<V> key, StreamerIndexUpdateSync sync) {
        assert key != null;
        assert isUnique();

        idxLocks.remove(key, sync);
    }

    /**
     * @param key Key,
     * @param val Value.
     * @param idxKey Index key.
     * @param evt Event.
     * @return Entry.
     */
    protected Entry<E, K, V> newEntry(K key, V val, @Nullable IndexKey<V> idxKey, E evt) {
        StreamerIndexPolicy plc = getPolicy();

        switch (plc) {
            case EVENT_TRACKING_OFF:
                return new NonTrackingEntry<>(key, val, idxKey);

            case EVENT_TRACKING_ON:
                return new EventTrackingEntry<>(addToCollection(null, evt), key, val, idxKey);

            default:
                assert plc == EVENT_TRACKING_ON_DEDUP : "Unknown policy: " + plc;

                return new DedupTrackingEntry<>(addToMap(null, evt), key, val, idxKey);
        }
    }

    /**
     * @param oldEntry Old entry.
     * @param key Key,
     * @param val Value.
     * @param idxKey Index key.
     * @param evt Event.
     * @return Entry.
     */
    protected Entry<E, K, V> addEvent(StreamerIndexEntry<E,K,V> oldEntry, K key, V val,
        @Nullable IndexKey<V> idxKey, E evt) {
        StreamerIndexPolicy plc = getPolicy();

        switch (plc) {
            case EVENT_TRACKING_OFF:
                return new NonTrackingEntry<>(key, val, idxKey);

            case EVENT_TRACKING_ON:
                return new EventTrackingEntry<>(addToCollection(oldEntry.events(), evt), key, val, idxKey);

            default:
                assert plc == EVENT_TRACKING_ON_DEDUP : "Unknown policy: " + plc;

                return new DedupTrackingEntry<>(addToMap(((DedupTrackingEntry<E, K, V>)oldEntry).rawEvents(), evt),
                    key, val, idxKey);
        }
    }

    /**
     * @param oldEntry Old entry.
     * @param key Key,
     * @param val Value.
     * @param idxKey Index key.
     * @param evt Event.
     * @return Entry.
     */
    protected Entry<E, K, V> removeEvent(StreamerIndexEntry<E, K, V> oldEntry, K key, V val,
        @Nullable IndexKey<V> idxKey, E evt) {
        StreamerIndexPolicy plc = getPolicy();

        switch (plc) {
            case EVENT_TRACKING_OFF:
                return new NonTrackingEntry<>(key, val, idxKey);

            case EVENT_TRACKING_ON:
                Collection<E> oldEvts = oldEntry.events();

                assert oldEvts != null; // Event tracking is on.

                Collection<E> newEvts = removeFromCollection(oldEvts, evt);

                return new EventTrackingEntry<>(newEvts != null ? newEvts : oldEvts, key, val, idxKey);

            default:
                assert plc == EVENT_TRACKING_ON_DEDUP : "Unknown policy: " + plc;

                Map<E, Integer> oldMap = ((DedupTrackingEntry<E, K, V>)oldEntry).rawEvents();

                assert oldMap != null; // Event tracking is on.

                Map<E, Integer> newMap = removeFromMap(oldMap, evt);

                return new DedupTrackingEntry<>(newMap != null ? newMap : oldMap, key, val, idxKey);
        }
    }

    /**
     * @param col Collection.
     * @param evt Event.
     * @return Cloned collection.
     */
    protected static <E> Collection<E> addToCollection(@Nullable Collection<E> col, E evt) {
        PVector<E> res = col == null ? TreePVector.<E>empty() : (PVector<E>)col;

        return res.plus(evt);
    }

    /**
     * @param map Collection.
     * @param evt Event.
     * @return Cloned map.
     */
    protected static <E> Map<E, Integer> addToMap(@Nullable Map<E, Integer> map, E evt) {
        HashPMap<E, Integer> res = map == null ? HashTreePMap.<E, Integer>empty() : (HashPMap<E, Integer>)map;

        Integer cnt = res.get(evt);

        return cnt != null ? res.minus(evt).plus(evt, cnt + 1) : res.plus(evt, 1);
    }

    /**
     * @param col Collection.
     * @param evt Event.
     * @return Cloned collection.
     */
    @Nullable protected static <E> Collection<E> removeFromCollection(@Nullable Collection<E> col, E evt) {
        if (col == null)
            return null;

        PVector<E> res = (PVector<E>)col;

        res = res.minus(evt);

        return res.isEmpty() ? null : res;
    }

    /**
     * @param map Collection.
     * @param evt Event.
     * @return Cloned map.
     */
    @Nullable protected static <E> Map<E, Integer> removeFromMap(@Nullable Map<E, Integer> map, E evt) {
        if (map == null)
            return null;

        HashPMap<E, Integer> res = (HashPMap<E, Integer>)map;

        Integer cnt = res.get(evt);

        return cnt == null ? res : cnt == 1 ? res.minus(evt) : res.minus(evt).plus(evt, cnt - 1);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String updaterClass() {
        return updater.getClass().getName();
    }

    /** {@inheritDoc} */
    @Override public boolean unique() {
        return unique;
    }

    /** {@inheritDoc} */
    @Override public StreamerIndexPolicy policy() {
        return plc;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return index0().size();
    }

    /**
     * Validates that given index key has overridden equals and hashCode methods.
     *
     * @param key Index key.
     * @throws IllegalArgumentException If validation fails.
     */
    private void validateIndexKey(@Nullable Object key) {
        if (keyCheck) {
            keyCheck = false;

            if (key == null)
                return;

            if (!U.overridesEqualsAndHashCode(key))
                throw new IllegalArgumentException("Index key must override hashCode() and equals() methods: " +
                    key.getClass().getName());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerIndexProviderAdapter.class, this);
    }

    /**
     * Streamer window index key.
     */
    protected interface IndexKey<V> {
        /**
         * @return Value associated with this key.
         */
        public V value();
    }

    /**
     * Utility method to safely get values from TrieMap.
     * See: https://github.com/romix/java-concurrent-hash-trie-map/issues/4
     *
     * @param key Key.
     * @param map Trie map.
     * @return Value from map.
     */
    @SuppressWarnings({"IfMayBeConditional", "TypeMayBeWeakened"})
    protected static <K, V> V trieGet(K key, TrieMap<K, V> map) {
        Object r = map.get(key);

        if(r instanceof Some)
            return ((Some<V>)r).get ();
        else if(r instanceof None)
            return null;
        else
            return (V)r;
    }

    /**
     * Streamer window index entry.
     */
    protected abstract static class Entry<E, K, V> implements StreamerIndexEntry<E, K, V> {
        /** */
        private final K key;

        /** */
        private final V val;

        /** */
        private final IndexKey<V> idxKey;

        /**
         * @param key Key.
         * @param val Value.
         * @param idxKey Key index.
         */
        Entry(K key, V val, @Nullable IndexKey<V> idxKey) {
            assert key != null;
            assert val != null;
            assert idxKey == null || idxKey.value() == val : "Keys are invalid [idxKey=" + idxKey + ", val=" + val +']';

            this.key = key;
            this.val = val;
            this.idxKey = idxKey;
        }

        /** {@inheritDoc} */
        @Override public K key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V value() {
            return val;
        }

        /**
         * @return Internal key.
         */
        @Nullable public IndexKey<V> keyIndex() {
            return idxKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof Entry))
                return false;

            StreamerIndexEntry<E, K, V> e = (StreamerIndexEntry<E, K, V>)obj;

            return key.equals(e.key());
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Entry.class, this, "identity", System.identityHashCode(this));
        }
    }

    /**
     * Entry with index policy {@link StreamerIndexPolicy#EVENT_TRACKING_OFF}.
     */
    protected static class NonTrackingEntry<E, K, V> extends Entry<E, K, V> {
        /**
         * @param key Key.
         * @param val Value.
         * @param idxKey Key index.
         */
        public NonTrackingEntry(K key, V val, @Nullable IndexKey<V> idxKey) {
            super(key, val, idxKey);
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NonTrackingEntry.class, this, super.toString());
        }
    }

    /**
     * Entry with index policy {@link StreamerIndexPolicy#EVENT_TRACKING_ON}.
     */
    protected static class EventTrackingEntry<E, K, V> extends Entry<E, K, V> {
        /** */
        private final Collection<E> evts;

        /**
         * @param evts Events.
         * @param key Key.
         * @param val Value.
         * @param idxKey Key index.
         */
        public EventTrackingEntry(Collection<E> evts, K key, V val, @Nullable IndexKey<V> idxKey) {
            super(key, val, idxKey);

            assert evts == null || !evts.isEmpty() : "Invalid events: " + evts;

            this.evts = evts;
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events() {
            return evts;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EventTrackingEntry.class, this, "evtCnt", evts.size(), "parent", super.toString());
        }
    }

    /**
     * Entry with index policy {@link StreamerIndexPolicy#EVENT_TRACKING_ON_DEDUP}.
     */
    protected static class DedupTrackingEntry<E, K, V> extends Entry<E, K, V> {
        /** */
        private final Map<E, Integer> evts;

        /**
         * @param evts Events.
         * @param key Key.
         * @param val Value.
         * @param idxKey Key index.
         */
        public DedupTrackingEntry(Map<E, Integer> evts, K key, V val, @Nullable IndexKey<V> idxKey) {
            super(key, val, idxKey);

            assert evts == null || !evts.isEmpty() : "Invalid events: " + evts;

            this.evts = evts;
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events() {
            return Collections.unmodifiableSet(evts.keySet());
        }

        /**
         * @return Events.
         */
        @Nullable public Map<E, Integer> rawEvents() {
            return evts;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DedupTrackingEntry.class, this, "evtCnt", evts.size(), "parent", super.toString());
        }
    }
}
