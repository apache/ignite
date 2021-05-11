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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;

/**
 * A wrapper for meta storage key represented by byte array.
 */
public final class Key implements Comparable<Key>, Serializable {
    /** Byte-wise representation of the key. */
    @NotNull
    private final byte[] arr;

    /**
     * Constructs key instance from the given string.
     *
     * @param s The string key representation. Can't be {@code null}.
     */
    public Key(@NotNull String s) {
        this(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Constructs key instance from the given byte array. <em>Note:</em> copy of the given byte array will not be
     * created in order to avoid redundant memory consumption.
     *
     * @param arr Byte array. Can't be {@code null}.
     */
    public Key(@NotNull byte[] arr) {
        this.arr = arr;
    }

    /**
     * Returns the key as byte array.
     *
     * @return Bytes of the key.
     */
    public byte[] bytes() {
        return arr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Key key = (Key)o;

        return Arrays.equals(arr, key.arr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(arr);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull Key other) {
        return Arrays.compare(this.arr, other.arr);
    }
}
