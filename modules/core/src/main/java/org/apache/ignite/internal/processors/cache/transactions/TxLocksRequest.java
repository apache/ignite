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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Transactions lock list request.
 */
public class TxLocksRequest extends GridCacheMessage {
    /** Future ID. */
    @Order(0)
    long futId;

    /** Tx keys. */
    @GridToStringInclude
    @Order(1)
    Set<IgniteTxKey> txKeys;

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

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
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
}
