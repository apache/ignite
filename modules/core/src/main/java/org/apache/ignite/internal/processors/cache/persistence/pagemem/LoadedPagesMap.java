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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.util.lang.GridPredicate3;

/**
 * Interface for storing correspondence of page id in a cache group
 * ->long value (address in offheap segment).
 *
 * This mapping is not thread safe and should be protected by outside locking.
 */
public interface LoadedPagesMap {
    /**
     *  Gets value associated with the given key.
     *
     * @param grpId Cache Group ID.
     * @param pageId Page ID.
     * @param ver Version, counter associated with value.
     * @param absent return if provided page is not presented in map.
     * @param outdated return if provided {@code ver} counter is greater than initial provided
     * @return A value associated with the given key.
     */
    public long get(int grpId, long pageId, int ver, long absent, long outdated);

    /**
     * Associates the given key with the given value.
     *
     * @param cacheId Cache ID
     * @param pageId Page ID.
     * @param val Value to set.
     * @param ver Version/counter associated with value, can be used to check if value is outdated.
     */
    public void put(int cacheId, long pageId, long val, int ver);

    /**
     * Refresh outdated value.
     *
     * @param cacheId Cache Group ID.
     * @param pageId Page ID.
     * @param tag Partition tag.
     * @return A value associated with the given key.
     */
    public long refresh(int cacheId, long pageId, int tag);

    /**
     * Removes key-value association for the given key.
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return {@code True} if value was actually found and removed.
     */
    public boolean remove(int grpId, long pageId);

    /**
     * @return Maximum number of entries in the map. This maximum can not be always reached.
     */
    public int capacity();


    /**
     * @return Current number of entries in the map.
     */
    public int size();

    /**
     * Find nearest presented value from specified position to the right.
     *
     * @param idxStart Index to start searching from. Bounded with {@link #capacity()}.
     * @return Closest value to the index and it's partition tag or  {@code null} value that will
     * be returned if no values present.
     */
    public ReplaceCandidate getNearestAt(int idxStart);

    /**
     * @param idx Index to clear value at. Bounded with {@link #capacity()}.
     * @param pred Test predicate.
     * @param absent Value to return if the cell is empty.
     * @return Value at the given index.
     */
    public long clearAt(int idx, GridPredicate3<Integer, Long, Integer> pred, long absent);
}
