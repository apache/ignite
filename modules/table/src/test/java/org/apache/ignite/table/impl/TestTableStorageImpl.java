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

package org.apache.ignite.table.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.internal.table.TableRow;

/**
 * Dummy table storage implementation.
 */
public class TestTableStorageImpl implements TableStorage {
    /** In-memory dummy store. */
    private final Map<KeyChunk, TestTableRowImpl> store = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public TableRow get(TableRow obj) {
        return store.get(new KeyChunk(obj.keyChunk().toBytes()));
    }

    /** {@inheritDoc} */
    @Override public TableRow put(TableRow row) {
        return store.put(
            new KeyChunk(row.keyChunk().toBytes()),
            new TestTableRowImpl(row.toBytes()));
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class KeyChunk {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyChunk(byte[] data) {
            this.data = data;
            this.hash = Arrays.hashCode(data);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyChunk wrapper = (KeyChunk)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }
}
