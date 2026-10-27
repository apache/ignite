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

/**
 */
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
     * @param oldRow Old row data.
     * @param newRow New row data.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    public void updateDataRow(
        T oldRow,
        T newRow,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException;

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
     * Reserves pages for a row (or a batch of rows) of the given size before it is inserted. Runs blocking eviction
     * when necessary (the caller must not hold any entry or page locks).
     *
     * @param rowSize Size of the row (or the total size of a batch of rows) to be inserted.
     * @param bestEffort {@code True} if the insert can not be safely refused (e.g. a backup update or a transaction
     *      commit): if the row does not fit into the region or the eviction makes no progress, the row is admitted
     *      without a reservation ({@code 0} is returned) and the page memory remains the hard limit. {@code False} if
     *      the insert can be refused: an out-of-memory is thrown instead.
     * @return Number of reserved pages to be released via {@link #releaseReserved(long)} once the row insert
     * completes, or {@code 0} if nothing is reserved (persistent data region, disabled page eviction or a best-effort
     * admission without a reservation).
     * @throws IgniteOutOfMemoryException If the insert is not best-effort and the row cannot fit into the region or
     *      eviction makes no progress. The exception is not reported to the failure handler: nothing has been changed
     *      yet, so the caller decides how to handle the refusal.
     * @throws IgniteCheckedException If failed to evict data pages.
     */
    public long reserve(long rowSize, boolean bestEffort) throws IgniteOutOfMemoryException, IgniteCheckedException;

    /**
     * Releases a reservation made by {@link #reserve(long, boolean)}.
     *
     * @param pages Number of pages to release (must not be negative).
     */
    public void releaseReserved(long pages);
}
