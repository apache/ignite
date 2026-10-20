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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.Storable;

/** */
public interface FreeList<T extends Storable> {
    /**
     * @param row Row.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    public void insertDataRow(T row, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param rows Rows.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    public void insertDataRows(Collection<T> rows, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @param row New row data.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateDataRow(long link, T row, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log);

    /**
     * Reserves pages for a row of the given size before the entry lock is acquired. Runs blocking eviction when
     * necessary (the caller must not hold any entry or page locks).
     *
     * @param rowSize Size of the row to be inserted.
     * @return Number of reserved pages to be released via {@link #releaseReserved(long)} once the row insert
     * completes, or {@code 0} if the reservation is not engaged for this free list (persistent data region or disabled page eviction).
     * @throws IgniteOutOfMemoryException If the row cannot fit into the region, or eviction makes no progress.
     * @throws IgniteCheckedException If failed to evict data pages.
     */
    public long reserve(int rowSize) throws IgniteOutOfMemoryException, IgniteCheckedException;

    /**
     * Releases the unconsumed portion of a reservation made by {@link #reserve(int)}.
     *
     * @param pages Number of pages to release (must not be negative).
     */
    public void releaseReserved(long pages);

    /**
     * Early out-of-memory check for an in-memory data region with disabled page eviction: verifies that the region
     * still has room for a row of the given size with a safety margin for free-list bookkeeping pages.
     * No-op for persistent regions or regions with eviction enabled.
     *
     * @param rowSize Row size in bytes.
     * @throws IgniteOutOfMemoryException If the region is near exhaustion.
     */
    public void checkFitsForInsert(int rowSize) throws IgniteOutOfMemoryException;
}
