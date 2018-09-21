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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cache entry transactional update result.
 */
public class GridCacheUpdateTxResult {
    /** Success flag.*/
    private final boolean success;

    /** Partition update counter. */
    private long updateCntr;

    /** */
    private GridLongList mvccWaitTxs;

    /** */
    private  GridFutureAdapter<GridCacheUpdateTxResult> fut;

    /** */
    private WALPointer logPtr;

    /** */
    private List<MvccLinkAwareSearchRow> mvccHistory;

    /**
     * Constructor.
     *
     * @param success Success flag.
     */
    GridCacheUpdateTxResult(boolean success) {
        this.success = success;
    }

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param logPtr Logger WAL pointer for the update.
     */
    GridCacheUpdateTxResult(boolean success, WALPointer logPtr) {
        this.success = success;
        this.logPtr = logPtr;
    }

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param fut Update future.
     */
    GridCacheUpdateTxResult(boolean success, GridFutureAdapter<GridCacheUpdateTxResult> fut) {
        this.success = success;
        this.fut = fut;
    }

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param updateCntr Update counter.
     * @param logPtr Logger WAL pointer for the update.
     */
    GridCacheUpdateTxResult(boolean success, long updateCntr, WALPointer logPtr) {
        this.success = success;
        this.updateCntr = updateCntr;
        this.logPtr = logPtr;
    }

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param updateCntr Update counter.
     * @param logPtr Logger WAL pointer for the update.
     * @param mvccWaitTxs List of transactions to wait for completion.
     */
    GridCacheUpdateTxResult(boolean success, long updateCntr, WALPointer logPtr, GridLongList mvccWaitTxs) {
        this.success = success;
        this.updateCntr = updateCntr;
        this.logPtr = logPtr;
        this.mvccWaitTxs = mvccWaitTxs;
    }

    /**
     * @return Partition update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return success;
    }

    /**
     * @return Logged WAL pointer for the update if persistence is enabled.
     */
    public WALPointer loggedPointer() {
        return logPtr;
    }

    /**
     * @return Update future.
     */
    @Nullable public IgniteInternalFuture<GridCacheUpdateTxResult> updateFuture() {
        return fut;
    }

    /**
     * @return List of transactions to wait for completion.
     */
    @Nullable public GridLongList mvccWaitTransactions() {
        return mvccWaitTxs;
    }

    /**
     *
     * @return Mvcc history rows.
     */
    @Nullable public List<MvccLinkAwareSearchRow> mvccHistory() {
        return mvccHistory;
    }

    /**
     *
     * @param mvccHistory Mvcc history rows.
     */
    public void mvccHistory(List<MvccLinkAwareSearchRow> mvccHistory) {
        this.mvccHistory = mvccHistory;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheUpdateTxResult.class, this);
    }
}
