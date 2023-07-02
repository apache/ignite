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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Represents committed transactions hash for a pair of nodes. */
public class TransactionsHashRecord extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Consistent ID of local node that participated in the transaction. This node produces this record. */
    @GridToStringInclude
    private Object locConsistentId;

    /** Consistent ID of remote node that participated in the transactions. */
    @GridToStringInclude
    private Object rmtConsistentId;

    /** Committed transactions IDs hash. */
    @GridToStringInclude
    private int txHash;

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
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(locConsistentId);
        out.writeObject(rmtConsistentId);
        out.writeInt(txHash);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        locConsistentId = in.readObject();
        rmtConsistentId = in.readObject();
        txHash = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsHashRecord.class, this);
    }
}
