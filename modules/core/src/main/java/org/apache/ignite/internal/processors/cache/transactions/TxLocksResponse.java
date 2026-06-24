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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;

/**
 * Transactions lock list response.
 */
public class TxLocksResponse extends GridCacheMessage implements MarshallableMessage {
    /** Future ID. */
    @Order(0)
    long futId;

    /** Locks for near txKeys of near transactions. */
    @GridToStringInclude
    private final Map<IgniteTxKey, List<TxLock>> nearTxKeyLocks = new HashMap<>();

    /** Remote keys involved into transactions. Doesn't include near keys. */
    @GridToStringInclude
    private Set<IgniteTxKey> txKeys;

    /** Wire-protocol array of keys from {@link #nearTxKeyLocks}. */
    @GridToStringExclude
    @Order(1)
    IgniteTxKey[] nearTxKeysArr;

    /** Wire-protocol array for {@link #txKeys}. */
    @GridToStringExclude
    @Order(2)
    IgniteTxKey[] txKeysArr;

    /** Wire-protocol array of values from {@link #nearTxKeyLocks}. */
    @GridToStringExclude
    @Order(3)
    List<TxLock>[] locksArr;

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
     * @return Lock lists for all near tx keys.
     */
    public Map<IgniteTxKey, List<TxLock>> txLocks() {
        if (nearTxKeysArr != null) {
            for (int i = 0; i < nearTxKeysArr.length; i++) {
                IgniteTxKey txKey = nearTxKeysArr[i];

                try {
                    nearTxKeyLocks.put(txKey, locksArr[i]);
                }
                catch (IllegalStateException ignored) {
                    // Skipping entries for missed cache.
                }
            }

            nearTxKeysArr = null;
            locksArr = null;
        }

        return nearTxKeyLocks;
    }

    /**
     * @param txKey Tx key.
     * @return Lock list for given tx key.
     */
    public List<TxLock> txLocks(IgniteTxKey txKey) {
        return txLocks().get(txKey);
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
        if (txKeysArr != null) {
            txKeys = U.newHashSet(txKeysArr.length);

            for (IgniteTxKey txKey : txKeysArr) {
                try {
                    txKeys.add(txKey);
                }
                catch (IllegalStateException ignored) {
                    // Skipping entries for removed cache.
                }
            }

            txKeysArr = null;
        }

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
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLocksResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (nearTxKeyLocks != null && !nearTxKeyLocks.isEmpty()) {
            int len = nearTxKeyLocks.size();

            nearTxKeysArr = new IgniteTxKey[len];
            locksArr = (List<TxLock>[])new List[len];

            int i = 0;

            for (Map.Entry<IgniteTxKey, List<TxLock>> entry : nearTxKeyLocks.entrySet()) {
                IgniteTxKey key = entry.getKey();

                nearTxKeysArr[i] = key;
                locksArr[i] = entry.getValue();

                i++;
            }
        }

        if (txKeys != null && !txKeys.isEmpty()) {
            txKeysArr = new IgniteTxKey[txKeys.size()];

            int i = 0;

            for (IgniteTxKey key : txKeys)
                txKeysArr[i++] = key;
        }
    }

}
