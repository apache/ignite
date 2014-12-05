/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.index.hash;

import com.romix.scala.collection.concurrent.*;
import org.gridgain.grid.*;
import org.gridgain.grid.streamer.index.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.streamer.index.StreamerIndexPolicy.*;

/**
 * Hash index implementation of a {@link org.gridgain.grid.streamer.index.StreamerIndexProvider}.
 * <p>
 * This implementation uses a concurrent hash map, which is extremely
 * efficient for CRUD operations. It does not, however, maintain the
 * ordering of entries, so, operations which imply ordering are not
 * supported.
 * <p>
 * If ordering is required, consider using {@link org.gridgain.grid.streamer.index.tree.StreamerTreeIndexProvider}.
 *
 * @see org.gridgain.grid.streamer.index.tree.StreamerTreeIndexProvider
 *
 */
public class StreamerHashIndexProvider<E, K, V> extends StreamerIndexProviderAdapter<E, K, V> {
    /** */
    private TrieMap<K, Entry<E, K, V>> key2Entry;

    /** */
    private final ThreadLocal<State<E, K, V>> state = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected StreamerIndex<E, K, V> index0() {
        return new Index<>();
    }

    /** {@inheritDoc} */
    @Override public void initialize() {
        key2Entry = new TrieMap<>();
    }

    /** {@inheritDoc} */
    @Override public void reset0() {
        // This will recreate maps.
        initialize();
    }

    /** {@inheritDoc} */
    @Override protected void add(E evt, K key, StreamerIndexUpdateSync sync) throws GridException {
        State<E, K, V> state0 = state.get();

        if (state0 != null)
            throw new IllegalStateException("Previous operation has not been finished: " + state0);

        Entry<E, K, V> oldEntry = trieGet(key, key2Entry);

        StreamerIndexUpdater<E, K, V> updater = getUpdater();

        if (oldEntry == null) {
            V val = updater.initialValue(evt, key);

            if (val == null)
                return; // Ignore event.

            state0 = new State<>(null, null, false);

            state.set(state0);

            Entry<E, K, V> newEntry = newEntry(key, val, null, evt);

            // Save new entry to state.
            state0.newEntry(newEntry);

            // Put new entry.
            Entry<E, K, V> rmv = key2Entry.put(key, newEntry);

            assert rmv == null;

            // Update passed.
            state0.finished(true);
        }
        else {
            if (isUnique())
                throw new GridException("Index unique key violation [evt=" + evt + ", key=" + key + ']');

            V val = updater.onAdded(oldEntry, evt);

            if (val == null) {
                remove(evt, key, sync);

                return;
            }

            state0 = new State<>(oldEntry, null, false);

            state.set(state0);

            Entry<E, K, V> newEntry = addEvent(oldEntry, key, val, null, evt);

            // Save new entry to state.
            state0.newEntry(newEntry);

            // Replace former entry with the new one.
            Entry<E, K, V> rmv = key2Entry.put(key, newEntry);

            assert rmv != null;

            // Update passed.
            state0.finished(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void remove(E evt, K key, StreamerIndexUpdateSync sync) throws GridException {
        State<E, K, V> state0 = state.get();

        if (state0 != null)
            throw new IllegalStateException("Previous operation has not been finished: " + state0);

        Entry<E, K, V> oldEntry = trieGet(key, key2Entry);

        if (oldEntry == null)
            return;

        StreamerIndexUpdater<E, K, V> updater = getUpdater();

        V val = updater.onRemoved(oldEntry, evt);

        if (val == null) {
            state0 = new State<>(oldEntry, null, false);

            state.set(state0);

            boolean b = key2Entry.remove(key, oldEntry);

            assert b;

            state0.finished(true);
        }
        else {
            state0 = new State<>(oldEntry, null, false);

            state.set(state0);

            Entry<E, K, V> newEntry = removeEvent(oldEntry, key, val, null, evt);

            // Save new entry to state.
            state0.newEntry(newEntry);

            // Replace former entry with the new one.
            Entry<E, K, V> rmv = key2Entry.put(key, newEntry);

            assert rmv != null;

            state0.finished(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void endUpdate0(StreamerIndexUpdateSync sync, E evt, K key, boolean rollback) {
        State<E, K, V> state0 = state.get();

        if (state0 == null)
            return;

        state.remove();

        if (rollback && state0.finished()) {
            Entry<E, K, V> oldEntry = state0.oldEntry();
            Entry<E, K, V> newEntry = state0.newEntry();

            // Rollback after index was updated.
            if (oldEntry != null && newEntry != null) {
                boolean b = key2Entry.replace(key, newEntry, oldEntry);

                assert b;
            }
            else if (newEntry == null) {
                // Old was removed. Need to put it back.
                Entry<E, K, V> old = key2Entry.put(key, oldEntry);

                assert old == null;
            }
            else {
                assert oldEntry == null;

                // New entry was added. Remove it.
                boolean b = key2Entry.remove(key, newEntry);

                assert b;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean sorted() {
        return false;
    }

    /**
     *
     */
    private class Index<I extends IndexKey<V>> implements StreamerIndex<E, K, V> {
        /** */
        private final TrieMap<K, Entry<E, K, V>> key2Entry0 = key2Entry.readOnlySnapshot();

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
            return false;
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
            A.ensure(cnt >= 0, "cnt >= 0");

            Collection vals = Collections.unmodifiableCollection(key2Entry0.values());

            return (Collection<StreamerIndexEntry<E, K, V>>)(cnt == 0 ? vals : F.limit(vals, cnt));
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet(int cnt) {
            A.ensure(cnt >= 0, "cnt >= 0");

            return cnt == 0 ? Collections.unmodifiableSet(key2Entry0.keySet()) :
                F.limit(key2Entry0.keySet(), cnt);
        }

        /** {@inheritDoc} */
        @Override public Collection<V> values(int cnt) {
            Collection<StreamerIndexEntry<E, K, V>> col = entries(cnt);

            return F.viewReadOnly(col, entryToVal);
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events(int cnt) {
            A.ensure(cnt >= 0, "cnt >= 0");

            if (getPolicy() == EVENT_TRACKING_OFF)
                throw new IllegalStateException("Event tracking is off: " + this);

            Collection<E> evts = new AbstractCollection<E>() {
                @NotNull @Override public Iterator<E> iterator() {
                    return new Iterator<E>() {
                        private final Iterator<Entry<E, K, V>> entryIter = key2Entry0.values().iterator();

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
                    return evtsCnt;
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


            return cnt == 0 ? evts : F.limit(evts, cnt);
        }

        /** {@inheritDoc} */
        @Override public Set<StreamerIndexEntry<E, K, V>> entrySet(V val) {
            return entrySet(true, val, true, val, true);
        }

        /** {@inheritDoc} */
        @Override public Set<StreamerIndexEntry<E, K, V>> entrySet(final boolean asc, @Nullable final V fromVal,
            final boolean fromIncl, @Nullable final V toVal, final boolean toIncl) {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet(V val) {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet(final boolean asc, @Nullable final V fromVal, final boolean fromIncl,
            @Nullable final V toVal, final boolean toIncl) {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Override public Collection<V> values(boolean asc, @Nullable V fromVal, boolean fromIncl, @Nullable V toVal,
            boolean toIncl) {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events(V val) {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Override public Collection<E> events(final boolean asc, @Nullable final V fromVal, final boolean fromIncl,
            @Nullable final V toVal, final boolean toIncl) {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndexEntry<E, K, V> firstEntry() {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndexEntry<E, K, V> lastEntry() {
            throw new UnsupportedOperationException("Operation is not supported on hash index.");
        }

        /** {@inheritDoc} */
        @Override public Iterator<StreamerIndexEntry<E, K, V>> iterator() {
            return entries(0).iterator();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Index.class, this, "provider", StreamerHashIndexProvider.this);
        }
    }

    /**
     *
     */
    private static class State<E, K, V> {
        /** */
        private Entry<E, K, V> oldEntry;

        /** */
        private Entry<E, K, V> newEntry;

        /** */
        private boolean finished;

        /**
         * @param oldEntry Old.
         * @param newEntry New.
         * @param finished Finished.
         */
        private State(@Nullable Entry<E, K, V> oldEntry, @Nullable Entry<E, K, V> newEntry, boolean finished) {
            this.oldEntry = oldEntry;
            this.newEntry = newEntry;
            this.finished = finished;
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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(State.class, this);
        }
    }
}
