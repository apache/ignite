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

package org.apache.ignite.internal.util;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.lang.*;

import java.util.*;

/**
 * Interface for ignite internal tree.
 */
public interface IgniteTree<L, T> {
    /**
     * Put value in this tree.
     *
     * @param value value to be associated with the specified key
     * @return the previous value associated with key
     */
    T put(T value) throws IgniteCheckedException;

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the
     * key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the
     * key
     */
    T findOne(L key) throws IgniteCheckedException;

    /**
     * Returns a cursor from lower to upper bounds inclusive.
     *
     * @param lower Lower bound or {@code null} if unbounded.
     * @param upper Upper bound or {@code null} if unbounded.
     * @return Cursor.
     */
    GridCursor<T> find(L lower, L upper) throws IgniteCheckedException;

    /**
     * Removes the mapping for a key from this tree if it is present.
     *
     * @param key key whose mapping is to be removed from the tree
     * @return the previous value associated with key, or null if there was no mapping for key.
     */
    T remove(L key) throws IgniteCheckedException;

    /**
     * Returns the number of elements in this tree.
     *
     * @return the number of elements in this tree
     */
    long size() throws IgniteCheckedException;
}
