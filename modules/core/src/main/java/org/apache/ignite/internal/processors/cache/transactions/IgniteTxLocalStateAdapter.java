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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public abstract class IgniteTxLocalStateAdapter implements IgniteTxLocalState {
    /**
     * @param cacheCtx Cache context.
     * @param tx Transaction.
     * @param commit {@code False} if transaction rolled back.
     */
    protected final void onTxEnd(GridCacheContext cacheCtx, IgniteInternalTx tx, boolean commit) {
        if (cacheCtx.cache().configuration().isStatisticsEnabled()) {
            // Convert start time from ms to ns.
            if (commit)
                cacheCtx.cache().metrics0().onTxCommit((U.currentTimeMillis() - tx.startTime()) * 1000);
            else
                cacheCtx.cache().metrics0().onTxRollback((U.currentTimeMillis() - tx.startTime()) * 1000);
        }
    }
}
