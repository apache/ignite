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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Logical data record indented for transaction (tx) related actions.<br>
 * This record is marker of begin, prepare, commit, and rollback transactions.
 */
public class TxRecord extends TimeStampRecord {
    /** Transaction state. */
    private TransactionState state;

    /** Global transaction identifier within cluster, assigned by transaction coordinator. */
    private GridCacheVersion nearXidVer;

    /** Transaction entries write topology version. */
    private GridCacheVersion writeVer;

    /**
     * Transaction participating nodes.
     *
     * Structure:
     * Primary node -> [Backup nodes...]
     **/
    @Nullable private Map<Object, Collection<Object>> participatingNodes;

    /** If transaction is remote, primary node for this backup node. */
    @Nullable private Object primaryNode;

    /**
     *
     * @param state Transaction state.
     * @param nearXidVer Transaction id.
     * @param writeVer Transaction entries write topology version.
     * @param participatingNodes Primary -> Backup nodes participating in transaction.
     * @param primaryNode Primary node.
     */
    public TxRecord(
        TransactionState state,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        @Nullable Map<Object, Collection<Object>> participatingNodes,
        @Nullable Object primaryNode
    ) {
        this.state = state;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.participatingNodes = participatingNodes;
        this.primaryNode = primaryNode;
    }

    /**
     * @param state Transaction state.
     * @param nearXidVer Transaction id.
     * @param writeVer Transaction entries write topology version.
     * @param participatingNodes Primary -> Backup nodes participating in transaction.
     * @param primaryNode Primary node.
     * @param timestamp TimeStamp.
     */
    public TxRecord(
        TransactionState state,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        @Nullable Map<Object, Collection<Object>> participatingNodes,
        @Nullable Object primaryNode,
        long timestamp
    ) {
        super(timestamp);

        this.state = state;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.participatingNodes = participatingNodes;
        this.primaryNode = primaryNode;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.TX_RECORD;
    }

    /**
     * @return Near xid version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void nearXidVersion(GridCacheVersion nearXidVer) {
        this.nearXidVer = nearXidVer;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @param writeVer DHT version.
     */
    public void dhtVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /**
     * @return Transaction state.
     */
    public TransactionState state() {
        return state;
    }

    /**
     * @param state Transaction state.
     */
    public void state(TransactionState state) {
        this.state = state;
    }

    /**
     * @return Primary -> backup participating nodes.
     */
    public Map<Object, Collection<Object>> participatingNodes() {
        return participatingNodes;
    }

    /**
     * @param participatingNodeIds Primary -> backup participating nodes.
     */
    public void participatingNodes(Map<Object, Collection<Object>> participatingNodeIds) {
        this.participatingNodes = participatingNodeIds;
    }

    /**
     * @return Is transaction remote for backup.
     */
    public boolean remote() {
        return primaryNode != null;
    }

    /**
     * @return Primary node for backup if transaction is remote.
     */
    @Nullable public Object primaryNode() {
        return primaryNode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxRecord.class, this, "super", super.toString());
    }
}
