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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public abstract class IgniteTxLocalStateAdapter implements IgniteTxLocalState {
    /** */
    private static final Function<Integer, Set<Integer>> CREATE_INT_SET = k -> new HashSet<>();

    /** */
    private Map<Integer, Set<Integer>> touchedParts;

    /**
     * @param cacheCtx Cache context.
     * @param tx Transaction.
     * @param commit {@code False} if transaction rolled back.
     */
    protected final void onTxEnd(GridCacheContext cacheCtx, IgniteInternalTx tx, boolean commit) {
        if (cacheCtx.statisticsEnabled()) {
            long durationNanos = U.millisToNanos(U.currentTimeMillis() - tx.startTime());

            if (commit)
                cacheCtx.cache().metrics0().onTxCommit(durationNanos);
            else
                cacheCtx.cache().metrics0().onTxRollback(durationNanos);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<Integer>> touchedPartitions() {
        Map<Integer, Set<Integer>> parts = touchedParts;

        return parts != null ? Collections.unmodifiableMap(parts) : null;
    }

    /** {@inheritDoc} */
    @Override public void touchPartition(int cacheId, int partId) {
        if (touchedParts == null)
            touchedParts = new HashMap<>();

        touchedParts.computeIfAbsent(cacheId, CREATE_INT_SET).add(partId);
    }
}
