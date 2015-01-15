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

package org.apache.ignite.streamer.index.tree;

import com.romix.scala.collection.concurrent.*;
import org.apache.ignite.*;
import org.apache.ignite.streamer.index.*;
import org.gridgain.grid.util.snaptree.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.streamer.index.StreamerIndexPolicy.*;

/**
 * Tree index implementation of a {@link org.apache.ignite.streamer.index.StreamerIndexProvider}.
 * <p>
 * The advantage of a tree index is that it maintains entries in a
 * sorted order, which is invaluable for many kinds of tasks, where
 * event ordering makes sense (like {@code GridStreamingPopularNumbersExample}).
 * The drawback is that the index entry values should be comparable to each other,
 * and you'll are likely to need to implement a custom comparator for values in
 * place of a default one.
 * <p>
 * If ordering is not required, consider using {@link org.apache.ignite.streamer.index.hash.StreamerHashIndexProvider}
 * instead, which is more efficient (O(1) vs. O(log(n))) and does not require
 * comparability.
 *
 * @see org.apache.ignite.streamer.index.hash.StreamerHashIndexProvider
 *
 */
public class StreamerTreeIndexProvider<E, K, V> extends StreamerIndexProviderAdapter<E, K, V> {
    /** */
    private SnapTreeMap<IndexKey<V>, Entry<E, K, V>> idx;

    /** */
    private TrieMap<K, Entry<E, K, V>> key2Entry;

    /** */
    private final AtomicLong idxGen = new AtomicLong();

    /** */
    private Comparator<V> cmp;

    /** */
    private final ThreadLocal<State<E, K, V>> state = new ThreadLocal<>();

    /**
     * Sets comparator.
     *
     * @param cmp Comparator.
     */
    public void setComparator(Comparator<V> cmp) {
        this.cmp = cmp;
    }

    /** {@inheritDoc} */
    @Override protected StreamerIndex<E, K, V> index0() {
        return new Index<>();
    }

    /** {@inheritDoc} */
    @Override public void initialize() {
        idx = cmp == null ? new SnapTreeMap<IndexKey<V>, Entry<E, K, V>>() :
            new SnapTreeMap<IndexKey<V>, Entry<E, K, V>>(new Comparator<IndexKey<V>>() {
                @Override public int compare(IndexKey<V> o1, IndexKey<V> o2) {
                    int res = cmp.compare(o1.value(), o2.value());

                    return res != 0 || isUnique() ? res :
                        ((Key<V>)o1).seed > ((Key<V>)o2).seed ? 1 :
                            ((Key<V>)o1).seed == ((Key<V>)o2).seed ? 0 : -1;
                }
            });

        key2Entry = new TrieMap<>();
    }

    /** {@inheritDoc} */
    @Override public void reset0() {
        // This will recreate maps.
        initialize();
    }

    /** {@inheritDoc} */
    @Override protected void add(E evt, K key, StreamerIndexUpdateSync sync) throws IgniteCheckedException {
        State<E, K, V> state0 = state.get();

        if (state0 != null)
            throw new IllegalStateException("Previous operation has not been finished: " + state0);

        Entry<E, K, V> oldEntry = trieGet(key, key2Entry);

        StreamerIndexUpdater<E, K, V> updater = getUpdater();

        if (oldEntry == null) {
            V val = updater.initialValue(evt, key);

            if (val == null)
                return; // Ignore event.

            IndexKey<V> idxKey = nextKey(val);

            state0 = new State<>(null, null, idxKey, null, false, false);

            if (isUnique())
                // Lock new key.
                lockIndexKey(idxKey, sync);

            state.set(state0);

            Entry<E, K, V> newEntry = newEntry(key, val, idxKey, evt);

            // Save new entry to state.
            state0.newEntry(newEntry);

            // Put new value to index.
            Entry<E, K, V> old = idx.putIfAbsent(idxKey, newEntry);

            if (isUnique()) {
                if (old != null)
                    throw new IgniteCheckedException("Index unique key violation [evt=" + evt + ", key=" + key +
                        ", idxKey=" + idxKey + ']');
            }
            else
                assert old == null;

            // Put new entry.
            Entry<E, K, V> rmv = key2Entry.put(key, newEntry);

            assert rmv == null;

            // Update passed.
            state0.finished(true);
        }
        else {
            V val = updater.onAdded(oldEntry, evt);

            if (val == null) {
                remove(evt, key, sync);

                return;
            }

            IndexKey<V> newIdxKey = nextKey(val);

            IndexKey<V> oldIdxKey = oldEntry.keyIndex();

            assert oldIdxKey != null; // Shouldn't be null for tree index.

            int order = compareKeys(oldIdxKey, newIdxKey);

            state0 = new State<>(oldIdxKey, oldEntry, newIdxKey, null, false, order == 0);

            if (isUnique()) {
                if (order == 0)
                    // Keys are equal.
                    lockIndexKey(newIdxKey, sync);
                else
                    lockKeys(oldIdxKey, newIdxKey, order, sync);
            }

            state.set(state0);

            Entry<E, K, V> newEntry = addEvent(oldEntry, key, val, newIdxKey, evt);

            // Save new entry to state.
            state0.newEntry(newEntry);

            if (state0.keysEqual()) {
                assert isUnique();

                boolean b = idx.replace(newIdxKey, oldEntry, newEntry);

                assert b;
            }
            else {
                // Put new value to index with new key.
                Entry<E, K, V> old = idx.putIfAbsent(newIdxKey, newEntry);

                if (isUnique()) {
                    if (old != null)
                        throw new IgniteCheckedException("Index unique key violation [evt=" + evt + ", key=" + key +
                            ", idxKey=" + newIdxKey + ']');
                }
                else
                    assert old == null;

                boolean rmv0 = idx.remove(oldIdxKey, oldEntry);

                assert rmv0;
            }

            // Replace former entry with the new one.
            boolean b = key2Entry.replace(key, oldEntry, newEntry);

            assert b;

            // Update passed.
            state0.finished(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void remove(E evt, K key, StreamerIndexUpdateSync sync) throws IgniteCheckedException {
        State<E, K, V> state0 = state.get();

        if (state0 != null)
            throw new IllegalStateException("Previous operation has not been finished: " + state0);

        Entry<E, K, V> oldEntry = trieGet(key, key2Entry);

        if (oldEntry == null)
            return;

        StreamerIndexUpdater<E, K, V> updater = getUpdater();

        V val = updater.onRemoved(oldEntry, evt);

        IndexKey<V> oldIdxKey = oldEntry.keyIndex();

        assert oldIdxKey != null; // Shouldn't be null for tree index.

        if (val == null) {
            state0 = new State<>(oldIdxKey, oldEntry, null, null, false, false);

            if (isUnique())
                // Lock old key.
                lockIndexKey(oldIdxKey, sync);

            state.set(state0);

            boolean b = idx.remove(oldIdxKey, oldEntry);

            assert b;

            b = key2Entry.remove(key, oldEntry);

            assert b;

            state0.finished(true);
        }
        else {
            IndexKey<V> newIdxKey = nextKey(val);

            int order = compareKeys(oldIdxKey, newIdxKey);

            state0 = new State<>(oldIdxKey, oldEntry, newIdxKey, null, false, order == 0);

            if (isUnique()) {
                if (order == 0)
                    // Keys are equal.
                    lockIndexKey(newIdxKey, sync);
                else
                    lockKeys(oldIdxKey, newIdxKey, order, sync);
            }

            state.set(state0);

            Entry<E, K, V> newEntry = removeEvent(oldEntry, key, val, newIdxKey, evt);

            // Save new entry to state.
            state0.newEntry(newEntry);

            if (state0.keysEqual()) {
                assert isUnique();

                boolean b = idx.replace(newIdxKey, oldEntry, newEntry);

                assert b;
            }
            else {
                // Put new value to index with new key.
                Entry<E, K, V> old = idx.putIfAbsent(newIdxKey, newEntry);

                if (isUnique()) {
                    if (old != null)
                        throw new IgniteCheckedException("Index unique key violation [evt=" + evt + ", key=" + key +
                            ", idxKey=" + newIdxKey + ']');
                }
                else
                    assert old == null;

                boolean rmv0 = idx.remove(oldIdxKey, oldEntry);

                assert rmv0;
            }

            // Replace former entry with the new one.
            boolean b = key2Entry.replace(key, oldEntry, newEntry);

            assert b;

            state0.finished(true);
        }
    }

    /**
     * @param key1 Key.
     * @param key2 Key.
     * @param order Keys comparison result.
     * @param sync Sync.
     * @throws IgniteCheckedException If interrupted.
     */
    private void lockKeys(IndexKey<V> key1, IndexKey<V> key2, int order, StreamerIndexUpdateSync sync)
        throws IgniteCheckedException {
        assert isUnique();
        assert key1 != null;
        assert key2 != null;
        assert order != 0;

        boolean success = false;

        try {
            if (order > 0) {
                lockIndexKey(key1, sync);
                lockIndexKey(key2, sync);
            }
            else {
                // Reverse order.
                lockIndexKey(key2, sync);
                lockIndexKey(key1, sync);
            }

            success = true;
        }
        finally {
            if (!success) {
                unlockIndexKey(key1, sync);
                unlockIndexKey(key2, sync);
            }
        }
    }

    /**
     * @param key1 Key.
     * @param key2 Key.
     * @return Comparison result.
     */
    private int compareKeys(IndexKey<V> key1, IndexKey<V> key2) {
        assert key1 != null;
        assert key2 != null;

        return cmp != null ? cmp.compare(key1.value(), key2.value()) :
            ((Comparable<V>)key1.value()).compareTo(key2.value());
    }

    /** {@inheritDoc} */
    @Override protected void endUpdate0(StreamerIndexUpdateSync sync, E evt, K key, boolean rollback) {
        State<E, K, V> state0 = state.get();

        if (state0 == null)
            return;

        state.remove();

        IndexKey<V> oldIdxKey = state0.oldIndexKey();
        Entry<E, K, V> oldEntry = state0.oldEntry();
        IndexKey<V> newIdxKey = state0.newIndexKey();
        Entry<E, K, V> newEntry = state0.newEntry();

        if (rollback && state0.finished()) {
            // Rollback after index was updated.
            if (oldEntry != null && newEntry != null) {
                if (state0.keysEqual()) {
                    assert isUnique();

                    boolean b = idx.replace(oldIdxKey, newEntry, oldEntry);

                    assert b;
                }
                else {
                    boolean b = idx.remove(newIdxKey, newEntry);

                    assert b;

                    Entry<E, K, V> old = idx.put(oldIdxKey, oldEntry);

                    assert old == null;

                    b = key2Entry.replace(key, newEntry, oldEntry);

                    assert b;
                }
            }
            else if (newEntry == null) {
                // Old was removed. Need to put it back.
                Entry<E, K, V> old = key2Entry.put(key, oldEntry);

                assert old == null;

                old = idx.put(oldIdxKey, oldEntry);

                assert old == null;
            }
            else {
                assert oldEntry == null;

                // New entry was added. Remove it.
                boolean b = idx.remove(newIdxKey, newEntry);

                assert b;

                b = key2Entry.remove(key, newEntry);

                assert b;
            }
        }

        // Unlock only if unique.
        if (isUnique()) {
            if (oldIdxKey != null)
                unlockIndexKey(oldIdxKey, sync);

            if (state0.keysEqual())
                // No need to unlock second key.
                return;

            if (newIdxKey != null)
                unlockIndexKey(newIdxKey, sync);
        }
    }

    /**
     * @param val Value.
     * @return Index key.
     */
    protected IndexKey<V> nextKey(V val) {
        return new Key<>(val, isUnique() ? 0 : idxGen.incrementAndGet(), cmp);
    }

    /**
     * @param val Value.
     * @param asc {@code True} if ascending.
     * @param incl {@code True} if inclusive.
     * @return Key for search.
     */
    private IndexKey<V> searchKeyFrom(V val, boolean asc, boolean incl) {
        // (asc && incl) || (!asc && !incl) -> asc == incl
        return new Key<>(val, asc == incl ? Long.MIN_VALUE : Long.MAX_VALUE, cmp);
    }

    /**
     * @param val Value.
     * @param asc {@code True} if ascending.
     * @param incl {@code True} if inclusive.
     * @return Key for search.
     */
    private IndexKey<V> searchKeyTo(V val, boolean asc, boolean incl) {
        // (asc && incl) || (!asc && !incl) -> asc == incl
        return new Key<>(val, asc == incl ? Long.MAX_VALUE : Long.MIN_VALUE, cmp);
    }

    /** {@inheritDoc} */
    @Override public boolean sorted() {
        return true;
    }

    /**
     *
     */
    private static class Key<V> implements Comparable<Key<V>>, IndexKey<V> {
        /** */
        private final V val;

        /** */
        private final long seed;

        /** */
        private final Comparator<V> cmp;

        /**
         * @param val Value.
         * @param seed Seed.
         * @param cmp Comparator.
         */
        private Key(V val, long seed, @Nullable Comparator<V> cmp) {
            assert val != null;

            this.val = val;
            this.seed = seed;
            this.cmp = cmp;
        }

        /** {@inheritDoc} */
        @Override public V value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Key<V> o) {
            int res = cmp != null ? cmp.compare(val, o.val) : ((Comparable<V>)val).compareTo(o.val);

            return res == 0 ? (seed < o.seed ? -1 : seed > o.seed ? 1 : 0) : res;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * val.hashCode() + (int)(seed ^ (seed >>> 32));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != Key.class)
                return false;

            Key key = (Key)obj;

            return seed == key.seed && val.equals(key.val);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Key.class, this);
        }
    }

    /**
     *
     */
    private static class State<E, K, V> {
        /** */
        private IndexKey<V> oldIdxKey;

        /** */
        private Entry<E, K, V> oldEntry;

        /** */
        private IndexKey<V> newIdxKey;

        /** */
        private Entry<E, K, V> newEntry;

        /** */
        private boolean finished;

        /** */
        private final boolean keysEqual;

        /**
         * @param oldIdxKey Old index key.
         * @param oldEntry Old entry.
         * @param newIdxKey New Index key.
         * @param newEntry New entry.
         * @param finished Finished.
         * @param keysEqual {@code True} if keys are equal.
         */
        private State(@Nullable IndexKey<V> oldIdxKey, @Nullable Entry<E, K, V> oldEntry, @Nullable IndexKey<V> newIdxKey,
            @Nullable Entry<E, K, V> newEntry, boolean finished, boolean keysEqual) {
            this.oldIdxKey = oldIdxKey;
            this.oldEntry = oldEntry;
            this.newIdxKey = newIdxKey;
            this.newEntry = newEntry;
            this.finished = finished;
            this.keysEqual = keysEqual;
        }

        /**
         * @return Old index entry.
         */
        IndexKey<V> oldIndexKey() {
            return oldIdxKey;
        }

        /**
         * @param oldIdxKey Old index key.
         */
        void oldIndexKey(IndexKey<V> oldIdxKey) {
            this.oldIdxKey = oldIdxKey;
        }

        /**
         * @return Old.
         */
        Entry<E, K, V> oldEntry() {
            return oldEntry;
        }

        /**
         * @param oldEntry Old.
         */
        void oldEntry(Entry<E, K, V> oldEntry) {
            this.oldEntry = oldEntry;
        }

        /**
         * @return New index key.
         */
        IndexKey<V> newIndexKey() {
            return newIdxKey;
        }

        /**
         * @param newIdxKey New index key.
         */
        void newIndexKey(IndexKey<V> newIdxKey) {
            this.newIdxKey = newIdxKey;
        }

        /**
         * @return New.
         */
        Entry<E, K, V> newEntry() {
            return newEntry;
        }

        /**
         * @param newEntry New.
         */
        void newEntry(Entry<E, K, V> newEntry) {
            this.newEntry = newEntry;
        }

        /**
         * @return Finished.
         */
        boolean finished() {
            return finished;
        }

        /**
         * @param finished Finished.
         */
        void finished(boolean finished) {
            this.finished = finished;
        }

        /**
         * @return {@code True} if both keys are not null and are equal (as comparables).
         */
        boolean keysEqual() {
            return keysEqual;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(State.class, this);
        }
    }

    /**
     *
     */
    private class Index<I extends IndexKey<V>> implements StreamerIndex<E, K, V> {
        /** */
        private final TrieMap<K, Entry<E, K, V>> key2Entry0 = key2Entry.readOnlySnapshot();

        /** */
        private final SnapTreeMap<IndexKey<V>, Entry<E, K, V>> idx0 = idx.clone();

        /** */
        private final int evtsCnt = eventsCount();

        /** {@inheritDoc} */
        @Nullable @Override public String name() {
            return getName();
        }

        /** {@inheritDoc} */
        @Override public boolean unique() {
            return isUnique();
        }

        /** {@inheritDoc} */
        @Override public boolean sorted() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public StreamerIndexPolicy policy() {
            return getPolicy();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return key2Entry0.size();
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndexEntry<E, K, V> entry(K key) {
            A.notNull(key, "key");

            return trieGet(key, key2Entry0);
        }

        /** {@inheritDoc} */
        @Override public Collection<StreamerIndexEntry<E, K, V>> entries(int cnt) {
            Collection col = cnt >= 0 ? idx0.values() : idx0.descendingMap().values();

            return (Collection<StreamerIndexEntry<E, K, V>>)(cnt == 0 ? Collections.unmodifiableCollection(col) :
                F.limit(col, U.safeAbs(cnt)));
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet(final int cnt) {
            Set<K> col = new AbstractSet<K>() {
                private Collection<K> entries = F.viewReadOnly(
                    cnt >= 0 ? idx0.values() : idx0.descendingMap().values(),
                    entryToKey);

                @NotNull @Override public Iterator<K> iterator() {
                    return entries.iterator();
                }

                @Override public int size() {
                    return entries.size();
                }
            };

            return cnt == 0 ? col : F.limit(col, U.safeAbs(cnt));
        }

        /** {@inheritDoc} */
        @Override public Collection<V> values(int cnt) {
            Collection<StreamerIndexEntry<E, K, V>> col = entries(cnt);

            return F.viewReadOnly(col, entryToVal);
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events(int cnt) {
            Collection<E> evts = events(cnt >= 0, null, false, null, false);

            return cnt == 0 ? evts : F.limit(evts, U.safeAbs(cnt));
        }

        /** {@inheritDoc} */
        @Override public Set<StreamerIndexEntry<E, K, V>> entrySet(V val) {
            return entrySet(true, val, true, val, true);
        }

        /** {@inheritDoc} */
        @Override public Set<StreamerIndexEntry<E, K, V>> entrySet(final boolean asc, @Nullable final V fromVal,
            final boolean fromIncl, @Nullable final V toVal, final boolean toIncl) {
            Set<StreamerIndexEntry<E, K, V>> set = new AbstractSet<StreamerIndexEntry<E, K, V>>() {
                private Map<IndexKey<V>, Entry<E, K, V>> map = subMap(asc, fromVal, fromIncl, toVal, toIncl);

                @NotNull @Override public Iterator<StreamerIndexEntry<E, K, V>> iterator() {
                    Collection vals = map.values();

                    return (Iterator<StreamerIndexEntry<E, K, V>>)vals.iterator();
                }

                @Override public int size() {
                    return map.size();
                }
            };

            return Collections.unmodifiableSet(set);
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet(V val) {
            return keySet(true, val, true, val, true);
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet(final boolean asc, @Nullable final V fromVal, final boolean fromIncl,
            @Nullable final V toVal, final boolean toIncl) {
            Set<K> set = new AbstractSet<K>() {
                private Map<IndexKey<V>, Entry<E, K, V>> map = subMap(asc, fromVal, fromIncl, toVal, toIncl);

                @NotNull @Override public Iterator<K> iterator() {
                    return F.iterator(map.values(), entryToKey, true);
                }

                @Override public int size() {
                    return map.size();
                }
            };

            return Collections.unmodifiableSet(set);
        }

        /** {@inheritDoc} */
        @Override public Collection<V> values(boolean asc, @Nullable V fromVal, boolean fromIncl, @Nullable V toVal,
            boolean toIncl) {
            Map<IndexKey<V>, Entry<E, K, V>> map = subMap(asc, fromVal, fromIncl, toVal, toIncl);

            return F.viewReadOnly(map.values(), entryToVal);
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events(V val) {
            A.notNull(val, "val");

            return events(true, val, true, val, true);
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events(final boolean asc, @Nullable final V fromVal, final boolean fromIncl,
            @Nullable final V toVal, final boolean toIncl) {
            if (getPolicy() == EVENT_TRACKING_OFF)
                throw new IllegalStateException("Event tracking is off: " + this);

            Collection<E> evts = new AbstractCollection<E>() {
                private final Map<IndexKey<V>, Entry<E, K, V>> map = subMap(asc, fromVal, fromIncl, toVal, toIncl);

                private int size = -1;

                @NotNull @Override public Iterator<E> iterator() {
                    return new Iterator<E>() {
                        private final Iterator<Entry<E, K, V>> entryIter = map.values().iterator();

                        private Iterator<E> evtIter;

                        private boolean moved = true;

                        private boolean more;

                        @Override public boolean hasNext() {
                            if (!moved)
                                return more;

                            moved = false;

                            if (evtIter != null && evtIter.hasNext())
                                return more = true;

                            while (entryIter.hasNext()) {
                                evtIter = eventsIterator(entryIter.next());

                                if (evtIter.hasNext())
                                    return more = true;
                            }

                            return more = false;
                        }

                        @Override public E next() {
                            if (hasNext()) {
                                moved = true;

                                return evtIter.next();
                            }

                            throw new NoSuchElementException();
                        }

                        @Override public void remove() {
                            assert false;
                        }
                    };
                }

                @Override public int size() {
                    return size != -1 ? size :
                        fromVal == null && toVal == null ? (size = evtsCnt) : (size = F.size(iterator()));
                }

                /**
                 * @param entry Entry.
                 * @return Events iterator.
                 */
                @SuppressWarnings("fallthrough")
                Iterator<E> eventsIterator(StreamerIndexEntry<E,K,V> entry) {
                    switch (getPolicy()) {
                        case EVENT_TRACKING_ON:
                        case EVENT_TRACKING_ON_DEDUP:
                            Collection<E> evts = entry.events();

                            assert evts != null;

                            return evts.iterator();

                        default:
                            assert false;

                            throw new IllegalStateException("Event tracking is off: " + Index.this);
                    }
                }
            };

            return Collections.unmodifiableCollection(evts);
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndexEntry<E, K, V> firstEntry() {
            return idx0.firstEntry().getValue();
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndexEntry<E, K, V> lastEntry() {
            return idx0.lastEntry().getValue();
        }

        /** {@inheritDoc} */
        @Override public Iterator<StreamerIndexEntry<E, K, V>> iterator() {
            return entries(0).iterator();
        }

        /**
         * @param asc Ascending.
         * @param fromVal From.
         * @param fromIncl Include from.
         * @param toVal To.
         * @param toIncl Include to.
         * @return Map.
         */
        private Map<IndexKey<V>, Entry<E, K, V>> subMap(boolean asc, @Nullable V fromVal, boolean fromIncl,
            @Nullable V toVal, boolean toIncl) {
            if (fromVal != null && toVal != null) {
                int cmpRes = cmp != null ? cmp.compare(toVal, fromVal) : ((Comparable<V>)toVal).compareTo(fromVal);

                if ((asc && cmpRes < 0) || (!asc && cmpRes > 0))
                    throw new IllegalArgumentException("Boundaries are invalid [asc=" + asc + ", fromVal=" + fromVal +
                        ", toVal=" + toVal + ']');
            }

            if (idx0.isEmpty())
                return Collections.emptyMap();

            ConcurrentNavigableMap<IndexKey<V>,Entry<E,K,V>> map = asc ? idx0 : idx0.descendingMap();

            if (fromVal == null) {
                fromVal = map.firstKey().value();

                fromIncl = true;
            }

            if (toVal == null) {
                toVal = map.lastKey().value();

                toIncl = true;
            }

            return map.subMap(searchKeyFrom(fromVal, asc, fromIncl), fromIncl, searchKeyTo(toVal, asc, toIncl), toIncl);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Index.class, this, "provider", StreamerTreeIndexProvider.this);
        }
    }
}
