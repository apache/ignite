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
import java.util.List;
import org.jetbrains.annotations.NotNull;

// TODO: IGNITE-14389 Tmp, used instead of real KeyValueStorage interface from metastorage-server module.
/**
 *
 */
@SuppressWarnings("unused") public interface KeyValueStorage {
    /** */
    long revision();

    /** */
    long updateCounter();

    /** */
    @NotNull Entry get(byte[] key);

    /** */
    @NotNull Entry get(byte[] key, long rev);

    /** */
    @NotNull Collection<Entry> getAll(List<byte[]> keys);

    /** */
    @NotNull Collection<Entry> getAll(List<byte[]> keys, long revUpperBound);

    /** */
    void put(byte[] key, byte[] value);

    /** */
    @NotNull Entry getAndPut(byte[] key, byte[] value);

    /** */
    void putAll(List<byte[]> keys, List<byte[]> values);

    /** */
    @NotNull Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values);

    /** */
    void remove(byte[] key);

    /** */
    @NotNull Entry getAndRemove(byte[] key);

    /** */
    void removeAll(List<byte[]> key);

    /** */
    @NotNull Collection<Entry> getAndRemoveAll(List<byte[]> keys);

    /** */
    boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure);

    /** */
    Cursor<Entry> range(byte[] keyFrom, byte[] keyTo);

    /** */
    Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound);

    /** */
    Cursor<WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev);

    /** */
    Cursor<WatchEvent> watch(byte[] key, long rev);

    /** */
    Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev);

    /** */
    void compact();
}
