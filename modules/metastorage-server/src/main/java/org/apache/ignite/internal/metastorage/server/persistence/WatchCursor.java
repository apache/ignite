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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.EntryEvent;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.bytesToValue;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.checkIterator;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.longToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.rocksKeyToBytes;

/**
 * Subscription on updates of entries corresponding to the given keys range (where the upper bound is unlimited)
 * and starting from the given revision number.
 */
class WatchCursor implements Cursor<WatchEvent> {
    /** Storage. */
    private final RocksDBKeyValueStorage storage;

    /** Key predicate. */
    private final Predicate<byte[]> p;

    /** Iterator for this cursor. */
    private final Iterator<WatchEvent> it;

    /** Options for {@link #nativeIterator}. */
    private final ReadOptions options = new ReadOptions().setPrefixSameAsStart(true);

    /** RocksDB iterator. */
    private final RocksIterator nativeIterator;

    /**
     * Last matching revision.
     */
    private long lastRetRev;

    /**
     * Next matching revision. {@code -1} means that it has not been found yet or does not exist.
     */
    private long nextRetRev = -1;

    /**
     * Constructor.
     *
     * @param storage Storage.
     * @param rev Starting revision.
     * @param p Key predicate.
     */
    WatchCursor(RocksDBKeyValueStorage storage, long rev, Predicate<byte[]> p) {
        this.storage = storage;
        this.p = p;
        this.lastRetRev = rev - 1;
        this.nativeIterator = storage.newDataIterator(options);
        this.it = createIterator();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public WatchEvent next() {
        return it.next();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.closeAll(options, nativeIterator);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<WatchEvent> iterator() {
        return it;
    }

    /**
     * Creates an iterator for this cursor.
     *
     * @return Iterator.
     */
    @NotNull
    private Iterator<WatchEvent> createIterator() {
        return new Iterator<>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                storage.lock().readLock().lock();

                try {
                    if (nextRetRev != -1)
                        // Next revision is already calculated and is not -1, meaning that there is a set of keys
                        // matching the revision and the predicate.
                        return true;

                    while (true) {
                        long curRev = lastRetRev + 1;

                        byte[] revisionPrefix = longToBytes(curRev);

                        boolean empty = true;

                        if (!nativeIterator.isValid()) {
                            try {
                                nativeIterator.refresh();
                            }
                            catch (RocksDBException e) {
                                throw new IgniteInternalException(e);
                            }
                        }

                        // Check all keys by the revision to see if any one of them match the predicate.
                        for (nativeIterator.seek(revisionPrefix); nativeIterator.isValid(); nativeIterator.next()) {
                            empty = false;

                            byte[] key = rocksKeyToBytes(nativeIterator.key());

                            if (p.test(key)) {
                                // Current revision matches.
                                nextRetRev = curRev;

                                return true;
                            }
                        }

                        checkIterator(nativeIterator);

                        if (empty)
                            return false;

                        // Go to the next revision.
                        lastRetRev++;
                    }
                }
                finally {
                    storage.lock().readLock().unlock();
                }
            }

            /** {@inheritDoc} */
            @Nullable
            @Override public WatchEvent next() {
                storage.lock().readLock().lock();

                try {
                    while (true) {
                        if (!hasNext())
                            return null;

                        var ref = new Object() {
                            boolean noItemsInRevision = true;
                        };

                        List<EntryEvent> evts = new ArrayList<>();

                        // Iterate over the keys of the current revision and get all matching entries.
                        RocksStorageUtils.forEach(nativeIterator, (k, v) -> {
                            ref.noItemsInRevision = false;

                            byte[] key = rocksKeyToBytes(k);

                            Value val = bytesToValue(v);

                            if (p.test(key)) {
                                Entry newEntry;

                                if (val.tombstone())
                                    newEntry = Entry.tombstone(key, nextRetRev, val.updateCounter());
                                else
                                    newEntry = new Entry(key, val.bytes(), nextRetRev, val.updateCounter());

                                Entry oldEntry = storage.doGet(key, nextRetRev - 1, false);

                                evts.add(new EntryEvent(oldEntry, newEntry));
                            }
                        });

                        if (ref.noItemsInRevision)
                            return null;

                        if (evts.isEmpty())
                            continue;

                        // Set the last returned revision to the current revision's value.
                        lastRetRev = nextRetRev;

                        // Set current revision to -1, meaning that it is not found yet.
                        nextRetRev = -1;

                        return new WatchEvent(evts);
                    }
                }
                catch (RocksDBException e) {
                    throw new IgniteInternalException(e);
                }
                finally {
                    storage.lock().readLock().unlock();
                }
            }
        };
    }
}
