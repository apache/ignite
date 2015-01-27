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

package org.apache.ignite.cache;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Enumeration of all supported cache peek modes. Peek modes can be passed into various
 * {@code 'CacheProjection.peek(..)'} and {@code CacheEntry.peek(..)} methods,
 * such as {@link CacheProjection#peek(Object, Collection)},
 * {@link CacheEntry#peek()}, and others.
 * <p>
 * The following modes are supported:
 * <ul>
 * <li>{@link #TX}</li>
 * <li>{@link #GLOBAL}</li>
 * <li>{@link #SMART}</li>
 * <li>{@link #SWAP}</li>
 * <li>{@link #DB}</li>
 * </ul>
 */
public enum GridCachePeekMode {
    /** Peeks value only from in-transaction memory of an ongoing transaction, if any. */
    TX,

    /** Peeks at cache global (not in-transaction) memory. */
    GLOBAL,

    /**
     * In this mode value is peeked from in-transaction memory first using {@link #TX}
     * mode and then, if it has not been found there, {@link #GLOBAL} mode is used to
     * search in committed cached values.
     */
    SMART,

    /** Peeks value only from off-heap or cache swap storage without loading swapped value into cache. */
    SWAP,

    /** Peek value from the underlying persistent storage without loading this value into cache. */
    DB,

    /**
     * Peek value from near cache only (don't peek from partitioned cache).
     * In case of {@link CacheMode#LOCAL} or {@link CacheMode#REPLICATED} cache,
     * behaves as {@link #GLOBAL} mode.
     */
    NEAR_ONLY,

    /**
     * Peek value from partitioned cache only (skip near cache).
     * In case of {@link CacheMode#LOCAL} or {@link CacheMode#REPLICATED} cache,
     * behaves as {@link #GLOBAL} mode.
     */
    PARTITIONED_ONLY;

    /** Enumerated values. */
    private static final GridCachePeekMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCachePeekMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
