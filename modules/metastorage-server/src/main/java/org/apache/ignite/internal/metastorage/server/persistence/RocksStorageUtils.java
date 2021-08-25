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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.ignite.internal.metastorage.server.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

/**
 * Utility class for {@link RocksDBKeyValueStorage}.
 */
class RocksStorageUtils {
    /**
     * VarHandle that gives the access to the elements of a {@code byte[]} array viewed as if it
     * were a {@code long[]} array. Byte order must be little endian for a correct
     * lexicographic order comparison.
     */
    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(
        long[].class,
        ByteOrder.LITTLE_ENDIAN
    );

    /**
     * Converts a long value to a byte array.
     *
     * @param value Value.
     * @return Byte array.
     */
    static byte[] longToBytes(long value) {
        var buffer = new byte[Long.BYTES];

        LONG_ARRAY_HANDLE.set(buffer, 0, value);

        return buffer;
    }

    /**
     * Converts a byte array to a long value.
     *
     * @param array Byte array.
     * @return Long value.
     */
    static long bytesToLong(byte[] array) {
        assert array.length == Long.BYTES;

        return (long) LONG_ARRAY_HANDLE.get(array, 0);
    }

    /**
     * Adds a revision to a key.
     *
     * @param revision Revision.
     * @param key Key.
     * @return Key with a revision.
     */
    static byte[] keyToRocksKey(long revision, byte[] key) {
        var buffer = new byte[Long.BYTES + key.length];

        LONG_ARRAY_HANDLE.set(buffer, 0, revision);

        System.arraycopy(key, 0, buffer, Long.BYTES, key.length);

        return buffer;
    }

    /**
     * Gets a key from a key with revision.
     *
     * @param rocksKey Key with a revision.
     * @return Key without a revision.
     */
    static byte[] rocksKeyToBytes(byte[] rocksKey) {
        // Copy bytes of the rocks key ignoring the revision (first 8 bytes)
        return Arrays.copyOfRange(rocksKey, Long.BYTES, rocksKey.length);
    }

    /**
     * Builds a value from a byte array.
     *
     * @param valueBytes Value byte array.
     * @return Value.
     */
    static Value bytesToValue(byte[] valueBytes) {
        // At least an 8-byte update counter and a 1-byte boolean
        assert valueBytes.length > Long.BYTES;

        // Read an update counter (8-byte long) from the entry.
        long updateCounter = (long) LONG_ARRAY_HANDLE.get(valueBytes, 0);

        // Read a has-value flag (1 byte) from the entry.
        boolean hasValue = valueBytes[Long.BYTES] != 0;

        byte[] val;
        if (hasValue)
            // Copy the value.
            val = Arrays.copyOfRange(valueBytes, Long.BYTES + 1, valueBytes.length);
        else
            // There is no value, mark it as a tombstone.
            val = TOMBSTONE;

        return new Value(val, updateCounter);
    }

    /**
     * Adds an update counter and a tombstone flag to a value.
     * @param value Value byte array.
     * @param updateCounter Update counter.
     * @return Value with an update counter and a tombstone.
     */
    static byte[] valueToBytes(byte[] value, long updateCounter) {
        var bytes = new byte[Long.BYTES + Byte.BYTES + value.length];

        LONG_ARRAY_HANDLE.set(bytes, 0, updateCounter);

        bytes[Long.BYTES] = (byte) (value == TOMBSTONE ? 0 : 1);

        System.arraycopy(value, 0, bytes, Long.BYTES + Byte.BYTES, value.length);

        return bytes;
    }

    /**
     * Gets an array of longs from the byte array of longs.
     *
     * @param bytes Byte array of longs.
     * @return Array of longs.
     */
    @NotNull
    static long[] getAsLongs(byte[] bytes) {
        // Value must be divisible by a size of a long, because it's a list of longs
        assert (bytes.length % Long.BYTES) == 0;

        return IntStream.range(0, bytes.length / Long.BYTES)
            .mapToLong(i -> (long) LONG_ARRAY_HANDLE.get(bytes, i * Long.BYTES))
            .toArray();
    }

    /**
     * Add a long value to an array of longs that is represented by an array of bytes.
     *
     * @param bytes Byte array that represents an array of longs.
     * @param value New long value.
     * @return Byte array with a new value.
     */
    static byte @NotNull [] appendLong(byte @Nullable [] bytes, long value) {
        if (bytes == null)
            return longToBytes(value);

        // Allocate a one long size bigger array
        var result = new byte[bytes.length + Long.BYTES];

        // Copy the current value
        System.arraycopy(bytes, 0, result, 0, bytes.length);

        LONG_ARRAY_HANDLE.set(result, bytes.length, value);

        return result;
    }
}
