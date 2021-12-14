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

package org.apache.ignite.internal.rocksdb;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Adapter from a {@link RocksIterator} to a {@link Cursor}.
 */
public abstract class RocksIteratorAdapter<T> implements Cursor<T> {
    /**
     * RocksDB iterator.
     */
    protected final RocksIterator it;

    /**
     * Constructor.
     *
     * @param it RocksDB iterator.
     */
    protected RocksIteratorAdapter(RocksIterator it) {
        this.it = it;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Iterator<T> iterator() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        it.close();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        boolean isValid = it.isValid();

        if (!isValid) {
            // check the status first. This operation is guaranteed to throw if an internal error has occurred during
            // the iteration. Otherwise we've exhausted the data range.
            try {
                it.status();
            } catch (RocksDBException e) {
                throw new IgniteInternalException(e);
            }
        }

        return isValid;
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        T result = decodeEntry(it.key(), it.value());

        it.next();

        return result;
    }

    /**
     * Converts the key and value, that the iterator is currently pointing at, into this cursor's value representation.
     *
     * <p>This method is called on each {@link #next()} method invocation.
     *
     * @param key Current DB key.
     * @param value Current DB value.
     * @return Cursor value representation.
     */
    protected abstract T decodeEntry(byte[] key, byte[] value);
}
