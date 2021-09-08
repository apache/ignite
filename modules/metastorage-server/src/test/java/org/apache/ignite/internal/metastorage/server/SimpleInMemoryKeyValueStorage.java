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

package org.apache.ignite.internal.metastorage.server;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

/**
 * Simple in-memory key/value storage.
 */
public class SimpleInMemoryKeyValueStorage implements KeyValueStorage {
    /** Lexicographical comparator. */
    private static final Comparator<byte[]> CMP = Arrays::compare;

    /**
     * Special value for revision number which means that operation should be applied
     * to the latest revision of an entry.
     */
    private static final long LATEST_REV = -1;

    /** Keys index. Value is the list of all revisions under which entry corresponding to the key was modified. */
    private NavigableMap<byte[], List<Long>> keysIdx = new TreeMap<>(CMP);

    /**  Revisions index. Value contains all entries which were modified under particular revision. */
    private NavigableMap<Long, NavigableMap<byte[], Value>> revsIdx = new TreeMap<>();

    /** Revision. Will be incremented for each single-entry or multi-entry update operation. */
    private long rev;

    /** Update counter. Will be incremented for each update of any particular entry. */
    private long updCntr;

    /** All operations are queued on this lock. */
    private final Object mux = new Object();

    /** {@inheritDoc} */
    @Override public void start() {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public long revision() {
        synchronized (mux) {
            return rev;
        }
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        synchronized (mux) {
            return updCntr;
        }
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] key, byte[] value) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPut(key, value, curRev);

            rev = curRev;
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndPut(byte[] key, byte[] bytes) {
        synchronized (mux) {
            long curRev = rev + 1;

            long lastRev = doPut(key, bytes, curRev);

            rev = curRev;

            // Return previous value.
            return doGetValue(key, lastRev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPutAll(curRev, keys, values);
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        Collection<Entry> res;

        synchronized (mux) {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            doPutAll(curRev, keys, values);
        }

        return res;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key) {
        synchronized (mux) {
            return doGet(key, LATEST_REV, false);
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key, long rev) {
        synchronized (mux) {
            return doGet(key, rev, true);
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAll(List<byte[]> keys) {
        return doGetAll(keys, LATEST_REV);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return doGetAll(keys, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public void remove(byte[] key) {
        synchronized (mux) {
            long curRev = rev + 1;

            if (doRemove(key, curRev))
                rev = curRev;
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndRemove(byte[] key) {
        synchronized (mux) {
            Entry e = doGet(key, LATEST_REV, false);

            if (e.empty() || e.tombstone())
                return e;

            return getAndPut(key, TOMBSTONE);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(List<byte[]> keys) {
        synchronized (mux) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV, false);

                if (e.empty() || e.tombstone())
                    continue;

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals);
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndRemoveAll(List<byte[]> keys) {
        Collection<Entry> res = new ArrayList<>(keys.size());

        synchronized (mux) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV, false);

                res.add(e);

                if (e.empty() || e.tombstone())
                    continue;

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        synchronized (mux) {
            Entry e = get(condition.key());

            boolean branch = condition.test(e);

            Collection<Operation> ops = branch ? success : failure;

            long curRev = rev + 1;

            boolean modified = false;

            for (Operation op : ops) {
                switch (op.type()) {
                    case PUT:
                        doPut(op.key(), op.value(), curRev);

                        modified = true;

                        break;

                    case REMOVE:
                        modified |= doRemove(op.key(), curRev);

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown operation type: " + op.type());
                }
            }

            if (modified)
                rev = curRev;

            return branch;
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo) {
        synchronized (mux) {
            return new RangeCursor(keyFrom, keyTo, rev);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
        return new RangeCursor(keyFrom, keyTo, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] keyFrom, byte @Nullable [] keyTo, long rev) {
        assert keyFrom != null : "keyFrom couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new WatchCursor(rev, k ->
            CMP.compare(keyFrom, k) <= 0 && (keyTo == null || CMP.compare(k, keyTo) < 0)
        );
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] key, long rev) {
        assert key != null : "key couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new WatchCursor(rev, k -> CMP.compare(k, key) == 0);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev) {
        assert keys != null && !keys.isEmpty() : "keys couldn't be null or empty: " + keys;
        assert rev > 0 : "rev must be positive.";

        TreeSet<byte[]> keySet = new TreeSet<>(CMP);

        keySet.addAll(keys);

        return new WatchCursor(rev, keySet::contains);
    }

    /** {@inheritDoc} */
    @Override public void compact() {
        synchronized (mux) {
            NavigableMap<byte[], List<Long>> compactedKeysIdx = new TreeMap<>(CMP);

            NavigableMap<Long, NavigableMap<byte[], Value>> compactedRevsIdx = new TreeMap<>();

            keysIdx.forEach((key, revs) -> compactForKey(key, revs, compactedKeysIdx, compactedRevsIdx));

            keysIdx = compactedKeysIdx;

            revsIdx = compactedRevsIdx;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path snapshotPath) {
        throw new UnsupportedOperationException();
    }

    /** */
    private boolean doRemove(byte[] key, long curRev) {
        Entry e = doGet(key, LATEST_REV, false);

        if (e.empty() || e.tombstone())
            return false;

        doPut(key, TOMBSTONE, curRev);

        return true;
    }

    /** */
    private void compactForKey(
            byte[] key,
            List<Long> revs,
            NavigableMap<byte[], List<Long>> compactedKeysIdx,
            NavigableMap<Long, NavigableMap<byte[], Value>> compactedRevsIdx
    ) {
        Long lastRev = lastRevision(revs);

        NavigableMap<byte[], Value> kv = revsIdx.get(lastRev);

        Value lastVal = kv.get(key);

        if (!lastVal.tombstone()) {
            compactedKeysIdx.put(key, listOf(lastRev));

            NavigableMap<byte[], Value> compactedKv = compactedRevsIdx.computeIfAbsent(
                    lastRev,
                    k -> new TreeMap<>(CMP)
            );

            compactedKv.put(key, lastVal);
        }
    }

    /** */
    @NotNull
    private Collection<Entry> doGetAll(List<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev > 0 || rev == LATEST_REV : "Revision must be positive or " + LATEST_REV + '.';

        Collection<Entry> res = new ArrayList<>(keys.size());

        synchronized (mux) {
            for (byte[] key : keys) {
                res.add(doGet(key, rev, false));
            }
        }

        return res;
    }

    /** */
    @NotNull
    private Entry doGet(byte[] key, long rev, boolean exactRev) {
        assert rev == LATEST_REV && !exactRev || rev > LATEST_REV :
                "Invalid arguments: [rev=" + rev + ", exactRev=" + exactRev + ']';

        List<Long> revs = keysIdx.get(key);

        if (revs == null || revs.isEmpty())
            return Entry.empty(key);

        long lastRev;

        if (rev == LATEST_REV)
            lastRev = lastRevision(revs);
        else
            lastRev = exactRev ? rev : maxRevision(revs, rev);

        // lastRev can be -1 if maxRevision return -1.
        if (lastRev == -1)
            return Entry.empty(key);

        return doGetValue(key, lastRev);
    }

    /**
     * Returns maximum revision which must be less or equal to {@code upperBoundRev}. If there is no such revision then
     * {@code -1} will be returned.
     *
     * @param revs Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Appropriate revision or {@code -1} if there is no such revision.
     */
    private static long maxRevision(List<Long> revs, long upperBoundRev) {
        int i = revs.size() - 1;

        for (; i >= 0; i--) {
            long rev = revs.get(i);

            if (rev <= upperBoundRev)
                return rev;
        }

        return -1;
    }

    /** */
    @NotNull
    private Entry doGetValue(byte[] key, long lastRev) {
        if (lastRev == 0)
            return Entry.empty(key);

        NavigableMap<byte[], Value> lastRevVals = revsIdx.get(lastRev);

        if (lastRevVals == null || lastRevVals.isEmpty())
            return Entry.empty(key);

        Value lastVal = lastRevVals.get(key);

        if (lastVal.tombstone())
            return Entry.tombstone(key, lastRev, lastVal.updateCounter());

        return new Entry(key, lastVal.bytes(), lastRev, lastVal.updateCounter());
    }

    /** */
    private long doPut(byte[] key, byte[] bytes, long curRev) {
        long curUpdCntr = ++updCntr;

        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        long lastRev = revs.isEmpty() ? 0 : lastRevision(revs);

        revs.add(curRev);

        // Update revsIdx.
        Value val = new Value(bytes, curUpdCntr);

        revsIdx.compute(
                curRev,
                (rev, entries) -> {
                    if (entries == null)
                        entries = new TreeMap<>(CMP);

                    entries.put(key, val);

                    return entries;
                }
        );

        return lastRev;
    }

    /** */
    private long doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList) {
        synchronized (mux) {
            // Update revsIdx.
            NavigableMap<byte[], Value> entries = new TreeMap<>(CMP);

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);

                byte[] bytes = bytesList.get(i);

                long curUpdCntr = ++updCntr;

                // Update keysIdx.
                List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

                revs.add(curRev);

                Value val = new Value(bytes, curUpdCntr);

                entries.put(key, val);

                revsIdx.put(curRev, entries);
            }

            rev = curRev;

            return curRev;
        }
    }

    /** */
    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    /** */
    private static List<Long> listOf(long val) {
        List<Long> res = new ArrayList<>();

        res.add(val);

        return res;
    }

    /** */
    private class RangeCursor implements Cursor<Entry> {
        /** */
        private final byte[] keyFrom;

        /** */
        private final byte[] keyTo;

        /** */
        private final long rev;

        /** */
        private final Iterator<Entry> it;

        /** */
        private Entry nextRetEntry;

        /** */
        private byte[] lastRetKey;

        /** */
        private boolean finished;

        /** */
        RangeCursor(byte[] keyFrom, byte[] keyTo, long rev) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.rev = rev;
            this.it = createIterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Entry next() {
            return it.next();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Entry> iterator() {
            return it;
        }

        @NotNull
        Iterator<Entry> createIterator() {
            return new Iterator<>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    synchronized (mux) {
                        while (true) {
                            if (finished)
                                return false;

                            if (nextRetEntry != null)
                                return true;

                            byte[] key = lastRetKey;

                            while (!finished || nextRetEntry == null) {
                                Map.Entry<byte[], List<Long>> e =
                                        key == null ? keysIdx.ceilingEntry(keyFrom) : keysIdx.higherEntry(key);

                                if (e == null) {
                                    finished = true;

                                    break;
                                }

                                key = e.getKey();

                                if (keyTo != null && CMP.compare(key, keyTo) >= 0) {
                                    finished = true;

                                    break;
                                }

                                List<Long> revs = e.getValue();

                                assert revs != null && !revs.isEmpty() :
                                        "Revisions should not be empty or null: [revs=" + revs + ']';

                                long lastRev = maxRevision(revs, rev);

                                if (lastRev == -1)
                                    continue;

                                Entry entry = doGetValue(key, lastRev);

                                assert !entry.empty() : "Iterator should not return empty entry.";

                                nextRetEntry = entry;

                                break;
                            }
                        }
                    }
                }

                /** {@inheritDoc} */
                @Override public Entry next() {
                    synchronized (mux) {
                        while (true) {
                            if (finished)
                                throw new NoSuchElementException();

                            if (nextRetEntry != null) {
                                Entry e = nextRetEntry;

                                nextRetEntry = null;

                                lastRetKey = e.key();

                                return e;
                            } else
                                hasNext();
                        }
                    }
                }
            };
        }
    }

    /** */
    private class WatchCursor implements Cursor<WatchEvent> {
        /** */
        private final Predicate<byte[]> p;

        /** */
        private final Iterator<WatchEvent> it;

        /** */
        private long lastRetRev;

        /** */
        private long nextRetRev = -1;

        /** */
        WatchCursor(long rev, Predicate<byte[]> p) {
            this.p = p;
            this.lastRetRev = rev - 1;
            this.it = createIterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public WatchEvent next() {
            return it.next();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<WatchEvent> iterator() {
            return it;
        }

        @NotNull
        Iterator<WatchEvent> createIterator() {
            return new Iterator<>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    synchronized (mux) {
                        if (nextRetRev != -1)
                            return true;

                        while (true) {
                            long curRev = lastRetRev + 1;

                            NavigableMap<byte[], Value> entries = revsIdx.get(curRev);

                            if (entries == null)
                                return false;

                            for (byte[] key : entries.keySet()) {
                                if (p.test(key)) {
                                    nextRetRev = curRev;

                                    return true;
                                }
                            }

                            lastRetRev++;
                        }
                    }
                }

                /** {@inheritDoc} */
                @Override public WatchEvent next() {
                    synchronized (mux) {
                        while (true) {
                            if (nextRetRev != -1) {
                                NavigableMap<byte[], Value> entries = revsIdx.get(nextRetRev);

                                if (entries == null)
                                    return null;

                                List<EntryEvent> evts = new ArrayList<>(entries.size());

                                for (Map.Entry<byte[], Value> e : entries.entrySet()) {
                                    byte[] key = e.getKey();

                                    Value val = e.getValue();

                                    if (p.test(key)) {
                                        Entry newEntry;

                                        if (val.tombstone())
                                            newEntry = Entry.tombstone(key, nextRetRev, val.updateCounter());
                                        else
                                            newEntry = new Entry(key, val.bytes(), nextRetRev, val.updateCounter());

                                        Entry oldEntry = doGet(key, nextRetRev - 1, false);

                                        evts.add(new EntryEvent(oldEntry, newEntry));
                                    }
                                }

                                if (evts.isEmpty())
                                    continue;

                                lastRetRev = nextRetRev;

                                nextRetRev = -1;

                                return new WatchEvent(evts);
                            } else if (!hasNext())
                                return null;
                        }
                    }
                }
            };
        }
    }
}
