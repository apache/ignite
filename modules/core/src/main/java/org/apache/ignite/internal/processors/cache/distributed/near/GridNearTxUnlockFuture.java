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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Waits for a failed lock attempt to be released and restores its initiating-side state.
 * A forced rollback can cancel this wait: transaction mappings are then retained for full rollback.
 */
public class GridNearTxUnlockFuture extends GridCompoundFuture<Void, Void> {
    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Entries enlisted by the lock attempt. */
    private final Collection<IgniteTxEntry> entries;

    /** Entry states before the attempt. */
    private final Map<KeyCacheObject, IgniteTxEntry> snapshots;

    /**
     * @param tx Transaction.
     * @param entries Entries enlisted by the lock attempt.
     * @param snapshots Entry states before the attempt.
     */
    public GridNearTxUnlockFuture(
        GridNearTxLocal tx,
        Collection<IgniteTxEntry> entries,
        Map<KeyCacheObject, IgniteTxEntry> snapshots
    ) {
        this.tx = tx;
        this.entries = entries;
        this.snapshots = snapshots;
    }

    /** {@inheritDoc} */
    @Override protected synchronized boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
        if (isDone())
            return false;

        // Serialize cancellation with successful local cleanup. Rollback must not observe half-restored mappings.
        if (err == null && !cancel) {
            try {
                if (tx.remainingTime() == -1)
                    throw tx.timeoutException();

                if (tx.isRollbackOnly())
                    throw tx.rollbackException();

                for (IgniteTxEntry entry : entries)
                    tx.removeFailedLockEntry(entry, snapshots.get(entry.key()));
            }
            catch (IgniteCheckedException | RuntimeException e) {
                err = e;
            }
        }

        if (err != null || cancel)
            tx.setRollbackOnly();

        tx.clearLockFuture(this);

        return super.onDone(res, err, cancel);
    }
}
