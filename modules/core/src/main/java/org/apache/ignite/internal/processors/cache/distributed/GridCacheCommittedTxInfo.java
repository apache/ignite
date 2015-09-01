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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Committed transaction information. Contains recovery writes that will be used to set commit values
 * in case if originating node crashes.
 */
public class GridCacheCommittedTxInfo implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Originating transaction ID. */
    private GridCacheVersion originatingTxId;

    /** Originating node ID. */
    private UUID originatingNodeId;

    /** Recovery writes, i.e. values that have never been sent to remote nodes. */
    @GridToStringInclude
    private Collection<IgniteTxEntry> recoveryWrites;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheCommittedTxInfo() {
        // No-op.
    }

    /**
     * @param tx Committed cache transaction.
     */
    public GridCacheCommittedTxInfo(IgniteInternalTx tx) {
        assert !tx.local() || !tx.replicated();

        originatingTxId = tx.nearXidVersion();
        originatingNodeId = tx.eventNodeId();
    }

    /**
     * @return Originating transaction ID (the transaction ID for replicated cache and near transaction ID
     *      for partitioned cache).
     */
    public GridCacheVersion originatingTxId() {
        return originatingTxId;
    }

    /**
     * @return Originating node ID (the local transaction node ID for replicated cache and near node ID
     *      for partitioned cache).
     */
    public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /**
     * @return Collection of recovery writes.
     */
    public Collection<IgniteTxEntry> recoveryWrites() {
        return recoveryWrites == null ? Collections.<IgniteTxEntry>emptyList() : recoveryWrites;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        originatingTxId.writeExternal(out);

        U.writeUuid(out, originatingNodeId);

        U.writeCollection(out, recoveryWrites);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        originatingTxId = new GridCacheVersion();

        originatingTxId.readExternal(in);

        originatingNodeId = U.readUuid(in);

        recoveryWrites = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCommittedTxInfo.class, this, "recoveryWrites", recoveryWrites);
    }
}