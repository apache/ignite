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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Transactions lock list response.
 */
public class TxLocksResponse extends GridCacheMessage {
    /** Future ID. */
    @Order(value = 3, method = "futureId")
    private long futId;

    /** Locks for near txKeys of near transactions. */
    @GridToStringInclude
    private final Map<IgniteTxKey, TxLockList> nearTxKeyLocks = new HashMap<>();

    /** Remote keys involved into transactions. Doesn't include near keys. */
    @GridToStringInclude
    private Set<IgniteTxKey> txKeys;

    /** Array of txKeys from {@link #nearTxKeyLocks}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    @Order(value = 4, method = "nearTxKeysArray")
    private IgniteTxKey[] nearTxKeysArr;

    /** Array of txKeys from {@link #txKeys}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    @Order(value = 5, method = "txKeysArray")
    private IgniteTxKey[] txKeysArr;

    /** Array of locksArr from {@link #nearTxKeyLocks}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    @Order(value = 6, method = "locksArray")
    private TxLockList[] locksArr;

    /**
     * Default constructor.
     */
    public TxLocksResponse() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
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
     * @return Array of txKeys from {@link #nearTxKeyLocks}. Used during marshalling and unmarshalling.
     */
    public IgniteTxKey[] nearTxKeysArray() {
        return nearTxKeysArr;
    }

    /**
     * @param nearTxKeysArr Array of txKeys from {@link #nearTxKeyLocks}. Used during marshalling and unmarshalling.
     */
    public void nearTxKeysArray(IgniteTxKey[] nearTxKeysArr) {
        this.nearTxKeysArr = nearTxKeysArr;
    }

    /**
     * @return Array of txKeys from {@link #txKeys}. Used during marshalling and unmarshalling.
     */
    public IgniteTxKey[] txKeysArray() {
        return txKeysArr;
    }

    /**
     * @param txKeysArr Array of txKeys from {@link #txKeys}. Used during marshalling and unmarshalling.
     */
    public void txKeysArray(IgniteTxKey[] txKeysArr) {
        this.txKeysArr = txKeysArr;
    }

    /**
     * @return Array of locksArr from {@link #nearTxKeyLocks}. Used during marshalling and unmarshalling.
     */
    public TxLockList[] locksArray() {
        return locksArr;
    }

    /**
     * @param locksArr Array of locksArr from {@link #nearTxKeyLocks}. Used during marshalling and unmarshalling.
     */
    public void locksArray(TxLockList[] locksArr) {
        this.locksArr = locksArr;
    }

    /**
     * @return Lock lists for all tx nearTxKeysArr.
     */
    public Map<IgniteTxKey, TxLockList> txLocks() {
        return nearTxKeyLocks;
    }

    /**
     * @param txKey Tx key.
     * @return Lock list for given tx key.
     */
    public TxLockList txLocks(IgniteTxKey txKey) {
        return nearTxKeyLocks.get(txKey);
    }

    /**
     * @param txKey Tx key.
     * @param txLock Tx lock.
     */
    public void addTxLock(IgniteTxKey txKey, TxLock txLock) {
        TxLockList lockList = nearTxKeyLocks.get(txKey);

        if (lockList == null)
            nearTxKeyLocks.put(txKey, lockList = new TxLockList());

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
            int len = nearTxKeyLocks.size();

            nearTxKeysArr = new IgniteTxKey[len];
            locksArr = new TxLockList[len];

            int i = 0;

            for (Map.Entry<IgniteTxKey, TxLockList> entry : nearTxKeyLocks.entrySet()) {
                IgniteTxKey key = entry.getKey();

                key.prepareMarshal(ctx.cacheContext(key.cacheId()));

                nearTxKeysArr[i] = key;
                locksArr[i] = entry.getValue();

                i++;
            }
        }

        if (txKeys != null && !txKeys.isEmpty()) {
            txKeysArr = new IgniteTxKey[txKeys.size()];

            int i = 0;

            for (IgniteTxKey key : txKeys) {
                key.prepareMarshal(ctx.cacheContext(key.cacheId()));

                txKeysArr[i++] = key;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        try {
            super.finishUnmarshal(ctx, ldr);

            if (nearTxKeysArr != null) {
                for (int i = 0; i < nearTxKeysArr.length; i++) {
                    IgniteTxKey txKey = nearTxKeysArr[i];

                    txKey.key().finishUnmarshal(ctx.cacheObjectContext(txKey.cacheId()), ldr);

                    txLocks().put(txKey, locksArr[i]);
                }

                nearTxKeysArr = null;
                locksArr = null;
            }

            if (txKeysArr != null) {
                txKeys = U.newHashSet(txKeysArr.length);

                for (IgniteTxKey txKey : txKeysArr) {
                    txKey.key().finishUnmarshal(ctx.cacheObjectContext(txKey.cacheId()), ldr);

                    txKeys.add(txKey);
                }

                txKeysArr = null;
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -23;
    }
}
