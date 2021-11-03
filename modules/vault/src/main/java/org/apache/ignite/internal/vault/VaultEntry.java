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

package org.apache.ignite.internal.vault;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a vault unit as entry with key and value, where
 * <ul>
 *     <li>key - an unique entry's key. Keys are comparable in lexicographic manner and represented as an
 *     array of bytes;</li>
 *     <li>value - a data which is associated with a key and represented as an array of bytes.</li>
 * </ul>
 */
public final class VaultEntry {
    /** Key. */
    private final ByteArray key;

    /** Value. */
    private final byte @Nullable [] val;

    /**
     * Constructs {@code VaultEntry} instance from the given key and value.
     *
     * @param key Key as a {@code ByteArray}. Cannot be null.
     * @param val Value as a {@code byte[]}.
     */
    public VaultEntry(ByteArray key, byte @Nullable [] val) {
        this.key = key;
        this.val = val;
    }

    /**
     * Returns a {@code ByteArray}.
     *
     * @return The {@code ByteArray}.
     */
    public ByteArray key() {
        return key;
    }

    /**
     * Returns a value. Can be {@code null} if the entry is empty.
     *
     * @return Value.
     */
    public byte @Nullable [] value() {
        return val;
    }

    /**
     * Returns value which denotes whether this entry is empty or not.
     *
     * @return {@code True} if entry is empty, otherwise - {@code false}.
     */
    public boolean empty() {
        return val == null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VaultEntry entry = (VaultEntry) o;
        return key.equals(entry.key) && Arrays.equals(val, entry.val);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = Objects.hash(key);
        result = 31 * result + Arrays.hashCode(val);
        return result;
    }
}
