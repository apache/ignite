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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cursor by entries which correspond to the given keys range.
 */
class RangeCursor implements Cursor<Entry> {
    /** Storage. */
    private final RocksDBKeyValueStorage storage;

    /** Lower iteration bound (included). */
    private final byte[] keyFrom;

    /** Upper iteration bound (excluded). */
    private final byte @Nullable [] keyTo;

    /** Revision upper bound (included). */
    private final long rev;

    /** Iterator. */
    private final Iterator<Entry> it;

    /** Next entry. */
    @Nullable
    private Entry nextRetEntry;

    /** Key of the last returned entry. */
    private byte[] lastRetKey;

    /**
     * {@code true} if the iteration is finished.
     */
    private boolean finished;

    /**
     * Constructor.
     *
     * @param storage Storage.
     * @param keyFrom {@link #keyFrom}.
     * @param keyTo {@link #keyTo}.
     * @param rev {@link #rev}.
     */
    RangeCursor(RocksDBKeyValueStorage storage, byte[] keyFrom, byte @Nullable [] keyTo, long rev) {
        this.storage = storage;
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

    /**
     * Creates an iterator for this cursor.
     *
     * @return Iterator.
     */
    @NotNull
    private Iterator<Entry> createIterator() {
        return new Iterator<>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                storage.lock().readLock().lock();

                try {
                    while (true) {
                        if (finished)
                            return false;

                        if (nextRetEntry != null)
                            return true;

                        byte[] key = lastRetKey;

                        while (nextRetEntry == null) {
                            Map.Entry<byte[], long[]> e =
                                key == null ? storage.revisionCeilingEntry(keyFrom) : storage.revisionHigherEntry(key);

                            if (e == null) {
                                finished = true;

                                break;
                            }

                            key = e.getKey();

                            if (keyTo != null && RocksDBKeyValueStorage.CMP.compare(key, keyTo) >= 0) {
                                finished = true;

                                break;
                            }

                            long[] revs = e.getValue();

                            assert revs != null && revs.length != 0 :
                                "Revisions should not be empty or null: [revs=" + Arrays.toString(revs) + ']';

                            long lastRev = RocksDBKeyValueStorage.maxRevision(revs, rev);

                            if (lastRev == -1)
                                continue;

                            Entry entry = storage.doGetValue(key, lastRev);

                            assert !entry.empty() : "Iterator should not return empty entry.";

                            nextRetEntry = entry;
                        }
                    }
                }
                finally {
                    storage.lock().readLock().unlock();
                }
            }

            /** {@inheritDoc} */
            @Override public Entry next() {
                storage.lock().readLock().lock();

                try {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    Entry e = nextRetEntry;

                    nextRetEntry = null;

                    assert e != null;

                    lastRetKey = e.key();

                    return e;
                }
                finally {
                    storage.lock().readLock().unlock();
                }
            }
        };
    }
}
