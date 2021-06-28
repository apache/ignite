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

package org.apache.ignite.lang;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A class wraps {@code byte[]} which provides {@link Object#equals}, {@link Object#hashCode} and
 * lexicographical comparison implementation.
 */
public final class ByteArray implements Comparable<ByteArray> {
    /** Wrapped byte array. */
    private final byte[] arr;

    /**
     * Constructs {@code ByteArray} instance from the given byte array.
     * <em>Note:</em> copy of the given byte array will not be
     * created in order to avoid redundant memory consumption.
     *
     * @param arr Byte array. Can't be {@code null}.
     */
    public ByteArray(byte[] arr) {
        this.arr = arr;
    }

    /**
     * Constructs {@code ByteArray} instance from the given string.
     *
     * @param s The string key representation. Can't be {@code null}.
     */
    public ByteArray(String s) {
        this(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Copy constructor, creates a shallow copy of the given {@code ByteArray}.
     *
     * @param other byte array to copy from
     */
    public ByteArray(ByteArray other) {
        arr = other.arr;
    }

    /**
     * Constructs {@code ByteArray} instance from the given string. {@link StandardCharsets#UTF_8} charset is used for
     * encoding the input string.
     *
     * @param s The string {@code ByteArray} representation. Can't be {@code null}.
     * @return {@code ByteArray} instance from the given string.
     */
    public static ByteArray fromString(String s) {
        return new ByteArray(s);
    }

    /**
     * Returns the {@code ByteArray} as byte array.
     *
     * @return Bytes of the {@code ByteArray}.
     */
    public byte[] bytes() {
        return arr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ByteArray byteArray = (ByteArray)o;

        return Arrays.equals(arr, byteArray.arr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(arr);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(ByteArray other) {
        return Arrays.compare(this.arr, other.arr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new String(arr, StandardCharsets.UTF_8);
    }
}
