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

package org.apache.ignite.internal.storage.basic;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.storage.DataRow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Basic array-based implementation of the {@link DataRow}.
 */
public class SimpleDataRow implements DataRow {
    /** Key array. */
    private final byte[] key;

    /** Value array. */
    private final byte @Nullable [] value;

    public SimpleDataRow(byte[] key, byte @Nullable [] value) {
        this.key = key;
        this.value = value;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public ByteBuffer key() {
        return ByteBuffer.wrap(key);
    }

    /** {@inheritDoc} */
    @Override public byte @NotNull [] keyBytes() {
        return key;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public ByteBuffer value() {
        return value == null ? null : ByteBuffer.wrap(value);
    }

    /** {@inheritDoc} */
    @Override public byte @Nullable [] valueBytes() {
        return value;
    }

    /** {@inheritDoc} */
    @Override public boolean hasValueBytes() {
        return value != null;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleDataRow row = (SimpleDataRow) o;
        return Arrays.equals(key, row.key) && Arrays.equals(value, row.value);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
