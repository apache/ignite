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

package org.apache.ignite.internal.vault.persistence;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Adapter from a {@link RocksIterator} to a {@link Cursor}.
 */
class RocksIteratorAdapter implements Cursor<VaultEntry> {
    /** */
    private final RocksIterator it;

    /**
     * Lower iteration bound. Needed for resource management.
     */
    private final Slice lowerBound;

    /**
     * Upper iteration bound. Needed for resource management.
     */
    private final Slice upperBound;

    /**
     * @param it RocksDB iterator
     * @param lowerBound lower iteration bound (included)
     * @param upperBound upper iteration bound (not included)
     */
    RocksIteratorAdapter(RocksIterator it, Slice lowerBound, Slice upperBound) {
        this.it = it;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<VaultEntry> iterator() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.closeAll(List.of(lowerBound, upperBound, it));
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        boolean isValid = it.isValid();

        if (!isValid) {
            // check the status first. This operation is guaranteed to throw if an internal error has occurred during
            // the iteration. Otherwise we've exhausted the data range.
            try {
                it.status();
            }
            catch (RocksDBException e) {
                throw new IgniteInternalException(e);
            }
        }

        return isValid;
    }

    /** {@inheritDoc} */
    @Override public VaultEntry next() {
        if (!hasNext())
            throw new NoSuchElementException();

        var result = new VaultEntry(new ByteArray(it.key()), it.value());

        it.next();

        return result;
    }
}
