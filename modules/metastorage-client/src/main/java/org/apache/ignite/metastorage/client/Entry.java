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

package org.apache.ignite.metastorage.client;

import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a storage unit as entry with key, value and revision, where
 * <ul>
 *     <li>key - an unique entry's key. Keys are comparable in lexicographic manner.</li>
 *     <li>value - a data which is associated with a key and represented as an array of bytes.</li>
 *     <li>revision - a number which denotes a version of whole meta storage. Each change increments the revision.</li>
 * </ul>
 */
public interface Entry {
    /**
     * Returns a key.
     *
     * @return The key.
     */
    @NotNull ByteArray key();

    /**
     * Returns a value. Could be {@code null} for empty entry.
     *
     * @return Value.
     */
    @Nullable byte[] value();

    /**
     * Returns a revision.
     *
     * @return Revision.
     */
    long revision();

    /**
     * Returns an update counter.
     *
     * @return Update counter.
     */
    long updateCounter();

    /**
     * Returns value which denotes whether entry is empty or not.
     *
     * @return {@code True} if entry is empty, otherwise - {@code false}.
     */
    boolean empty();

    /**
     * Returns value which denotes whether entry is tombstone or not.
     *
     * @return {@code True} if entry is tombstone, otherwise - {@code false}.
     */
    boolean tombstone();
}
