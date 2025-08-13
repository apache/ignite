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

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Transactions lock list request.
 */
public class TxLocksRequest extends GridCacheMessage {
    /** Future ID. */
    @Order(value = 3, method = "futureId")
    private long futId;

    /** Tx keys. */
    @GridToStringInclude
    private Set<IgniteTxKey> txKeys;

    /** Array of txKeys from {@link #txKeys}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    @Order(value = 4, method = "txKeysArray")
    private IgniteTxKey[] txKeysArr;

    /**
     * Default constructor.
     */
    public TxLocksRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param txKeys Target tx keys.
     */
    public TxLocksRequest(long futId, Set<IgniteTxKey> txKeys) {
        A.notEmpty(txKeys, "txKeys");

        this.futId = futId;
        this.txKeys = txKeys;
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
     * @return Tx keys.
     */
    public Collection<IgniteTxKey> txKeys() {
        return txKeys;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLocksRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        txKeysArr = new IgniteTxKey[txKeys.size()];

        int i = 0;

        for (IgniteTxKey key : txKeys) {
            key.prepareMarshal(ctx.cacheContext(key.cacheId()));

            txKeysArr[i++] = key;
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        txKeys = U.newHashSet(txKeysArr.length);

        for (IgniteTxKey key : txKeysArr) {
            key.finishUnmarshal(ctx.cacheContext(key.cacheId()), ldr);

            txKeys.add(key);
        }

        txKeysArr = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -24;
    }
}
