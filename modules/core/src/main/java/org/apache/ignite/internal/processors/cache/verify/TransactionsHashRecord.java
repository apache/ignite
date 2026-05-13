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

package org.apache.ignite.internal.processors.cache.verify;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

/** Represents committed transactions hash for a pair of nodes. */
public class TransactionsHashRecord implements MarshallableMessage, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Consistent ID of local node that participated in the transaction. This node produces this record. */
    @GridToStringInclude
    Object locConsistentId;

    /** Bytes of {@link #locConsistentId}. */
    @Order(0)
    byte[] locConsistentIdBytes;

    /** Consistent ID of remote node that participated in the transactions. */
    @GridToStringInclude
    Object rmtConsistentId;

    /** Bytes of {@link #rmtConsistentId}. */
    @Order(1)
    byte[] rmtConsistentIdBytes;

    /** Committed transactions IDs hash. */
    @Order(2)
    @GridToStringInclude
    int txHash;

    /** */
    public TransactionsHashRecord() {
        // No-op.
    }

    /** */
    public TransactionsHashRecord(Object locConsistentId, Object rmtConsistentId, int txHash) {
        this.locConsistentId = locConsistentId;
        this.rmtConsistentId = rmtConsistentId;
        this.txHash = txHash;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        locConsistentIdBytes = U.marshal(marsh, locConsistentId);
        rmtConsistentIdBytes = U.marshal(marsh, rmtConsistentId);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        locConsistentId = U.unmarshal(marsh, locConsistentIdBytes, clsLdr);
        rmtConsistentId = U.unmarshal(marsh, rmtConsistentIdBytes, clsLdr);
    }

    /** @return Committed transactions IDs hash. */
    public int transactionHash() {
        return txHash;
    }

    /** @return Consistent ID of remote node. */
    public Object remoteConsistentId() {
        return rmtConsistentId;
    }

    /** @return Consistent ID of local node. */
    public Object localConsistentId() {
        return locConsistentId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsHashRecord.class, this);
    }
}
