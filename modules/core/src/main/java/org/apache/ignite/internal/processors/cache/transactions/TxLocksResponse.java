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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Transactions lock list response.
 */
public class TxLocksResponse extends GridCacheMessage {
    /** Future ID. */
    @Order(0)
    long futId;

    /** Locks for near txKeys of near transactions. */
    @Order(1)
    @GridToStringInclude
    Map<IgniteTxKey, List<TxLock>> nearTxKeyLocks = new HashMap<>();

    /** Remote keys involved into transactions. Doesn't include near keys. */
    @Order(2)
    @GridToStringInclude
    Set<IgniteTxKey> txKeys;

    /**
     * Default constructor.
     */
    public TxLocksResponse() {
        // No-op.
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /**
     * @return Lock lists for all tx nearTxKeysArr.
     */
    public Map<IgniteTxKey, List<TxLock>> txLocks() {
        return nearTxKeyLocks;
    }

    /**
     * @param txKey Tx key.
     * @return Lock list for given tx key.
     */
    public List<TxLock> txLocks(IgniteTxKey txKey) {
        return nearTxKeyLocks.get(txKey);
    }

    /**
     * @param txKey Tx key.
     * @param txLock Tx lock.
     */
    public void addTxLock(IgniteTxKey txKey, TxLock txLock) {
        List<TxLock> lockList = nearTxKeyLocks.computeIfAbsent(txKey, k -> new ArrayList<>());

        lockList.add(txLock);
    }

    /**
     * @return Remote txKeys involved into tx.
     */
    public Set<IgniteTxKey> keys() {
        return txKeys;
    }

    /**
     * @param key Key.
     */
    public void addKey(IgniteTxKey key) {
        if (txKeys == null)
            txKeys = new HashSet<>();

        txKeys.add(key);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLocksResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearTxKeyLocks != null && !nearTxKeyLocks.isEmpty()) {
            for (Map.Entry<IgniteTxKey, List<TxLock>> entry : nearTxKeyLocks.entrySet()) {
                IgniteTxKey key = entry.getKey();

                key.prepareMarshal(ctx.cacheContext(key.cacheId()));
            }
        }

        if (txKeys != null && !txKeys.isEmpty()) {
            for (IgniteTxKey key : txKeys)
                key.prepareMarshal(ctx.cacheContext(key.cacheId()));
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        try {
            super.finishUnmarshal(ctx, ldr);

            if (nearTxKeyLocks != null && !nearTxKeyLocks.isEmpty()) {
                for (Map.Entry<IgniteTxKey, List<TxLock>> entry : nearTxKeyLocks.entrySet()) {
                    IgniteTxKey txKey = entry.getKey();

                    txKey.key().finishUnmarshal(ctx.cacheObjectContext(txKey.cacheId()), ldr);
                }
            }

            if (txKeys != null && !txKeys.isEmpty()) {
                for (IgniteTxKey txKey : txKeys)
                    txKey.key().finishUnmarshal(ctx.cacheObjectContext(txKey.cacheId()), ldr);
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

}
