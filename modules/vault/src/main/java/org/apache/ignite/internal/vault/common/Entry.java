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

package org.apache.ignite.internal.vault.common;

import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a vault unit as entry with key and value, where
 * <ul>
 *     <li>key - an unique entry's key. Keys are comparable in lexicographic manner and represented as an {@link ByteArray}.</li>
 *     <li>value - a data which is associated with a key and represented as an array of bytes.</li>
 * </ul>
 */
// TODO: need to generify with metastorage Entry https://issues.apache.org/jira/browse/IGNITE-14653
public final class Entry {
    /** Key. */
    private final ByteArray key;

    /** Value. */
    private final byte[] val;

    /**
     * Constructs {@code VaultEntry} instance from the given key and value.
     *
     * @param key Key as a {@code ByteArray}. Couldn't be null.
     * @param val Value as a {@code byte[]}.
     */
    public Entry(@NotNull ByteArray key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    /**
     * Returns a {@code ByteArray}.
     *
     * @return The {@code ByteArray}.
     */
    @NotNull public ByteArray key() {
        return key;
    }

    /**
     * Returns a value. Could be {@code null} for empty entry.
     *
     * @return Value.
     */
    @Nullable public byte[] value() {
        return val;
    }

    /**
     * Returns value which denotes whether entry is empty or not.
     *
     * @return {@code True} if entry is empty, otherwise - {@code false}.
     */
    public boolean empty() {
        return val == null;
    }
}
