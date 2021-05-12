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

package org.apache.ignite.metastorage.common;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.metastorage.common.DummyEntry;
import org.jetbrains.annotations.NotNull;

// TODO: IGNITE-14389 Tmp, should be removed.
/**
 *
 */
@SuppressWarnings("ConstantConditions")
public class KeyValueStorageImpl implements KeyValueStorage {
    /** {@inheritDoc} */
    @Override public long revision() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Entry get(byte[] key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Entry get(byte[] key, long rev) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<Entry> getAll(List<byte[]> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] key, byte[] value) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull Entry getAndPut(byte[] key, byte[] value) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void remove(byte[] key) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull Entry getAndRemove(byte[] key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void removeAll(List<byte[]> keys) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<Entry> getAndRemoveAll(List<byte[]> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
        return new Cursor<Entry>() {
            @NotNull @Override public Iterator<Entry> iterator() {
                return new Iterator<Entry>() {
                    @Override public boolean hasNext() {
                        return false;
                    }

                    @Override public Entry next() {
                        return null;
                    }
                };
            }

            @Override public void close() throws Exception {

            }
        };
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev) {
        return new Cursor<>() {
            /** {@inheritDoc} */
            @Override public void close(){

            }

            /** {@inheritDoc} */
            @NotNull @Override public Iterator<WatchEvent> iterator() {
                return new Iterator<>() {
                    @Override public boolean hasNext() {
                        return true;
                    }

                    @Override public WatchEvent next() {
                        return new WatchEvent(
                            new DummyEntry(
                                new Key(new byte[] {1}),
                                new byte[] {2},
                                1L,
                                1L
                            ),
                            new DummyEntry(
                                new Key(new byte[] {1}),
                                new byte[] {3},
                                2L,
                                2L
                            )
                        );
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] key, long rev) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void compact() {

    }
}
