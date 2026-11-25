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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * Entry point for per-page eviction. Accepts information about touching data pages,
 * capable of evicting "the least needed" page (according to implemented eviction algorithm).
 */
public interface PageEvictionTracker extends LifecycleAware {
    /**
     * Call this method when data page is accessed.
     *
     * @param pageId Page id.
     */
    public void touchPage(long pageId);

    /**
     * Evicts one data page.
     * In most cases, all entries will be removed from the page. Removing all entries may be
     * not possible, as some of them can be used by active transactions.
     *
     * @return {@code True} if all entries of a page were evicted, {@code false} otherwise.
     * @throws IgniteCheckedException In case of page memory error.
     */
    public boolean evictDataPage() throws IgniteCheckedException;

    /**
     * Call this method when last entry is removed from data page.
     *
     * @param pageId Page id.
     */
    public void forgetPage(long pageId);

    /**
     * Call this method when data page containing fragment of row is written.
     *
     * @param pageId     Page id.
     * @param prevPageId Page id of previous fragment.  0 if called for the tail fragment (written first).
     * @param isHeadPage True if head fragment (written last) of row is written, False otherwise.
     */
    public void trackFragmentPage(long pageId, long prevPageId, boolean isHeadPage);

    /**
     * @return {@code True} if this tracker reserves pages for inserts: only for in-memory regions with enabled page
     *      eviction.
     */
    public boolean reserveEnabled();

    /**
     * Reserves pages for a row (or a batch of rows) of the given size before it is inserted. Runs blocking eviction
     * when necessary (the caller must not hold any entry or page locks).
     *
     * @param rowSize Size of the row (or the total size of a batch of rows) to be inserted.
     * @param bestEffort {@code True} if the insert can not be safely refused (e.g. a backup update or a transaction
     *      commit): if the row does not fit into the region or the eviction makes no progress, the row is admitted
     *      without a reservation ({@code 0} is returned) and the page memory remains the hard limit: exceeding it fails
     *      the insert with a critical error reported to the failure handler. {@code False} if the insert can be
     *      refused: an out-of-memory is thrown instead.
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
