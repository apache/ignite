/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Logical data record indented for transaction (tx) related actions.<br>
 * This record is marker of begin, prepare, commit, and rollback transactions.
 */
public class TxRecord extends TimeStampRecord {
    /** Transaction state. */
    @GridToStringInclude
    private final TransactionState state;

    /** Global transaction identifier within cluster, assigned by transaction coordinator. */
    @GridToStringInclude
    private final GridCacheVersion nearXidVer;

    /** Transaction entries write topology version. */
    private final GridCacheVersion writeVer;

    /**
     * Transaction participating nodes.
     *
     * Structure:
     * Primary node -> [Backup nodes...], where nodes are identified by compact ID for some baseline topology.
     **/
    @Nullable private Map<Short, Collection<Short>> participatingNodes;

    /**
     *
     * @param state Transaction state.
     * @param nearXidVer Transaction id.
     * @param writeVer Transaction entries write topology version.
     * @param participatingNodes Primary -> Backup nodes compact IDs participating in transaction.
     */
    public TxRecord(
        TransactionState state,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        @Nullable Map<Short, Collection<Short>> participatingNodes
    ) {
        this.state = state;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.participatingNodes = participatingNodes;
    }

    /**
     * @param state Transaction state.
     * @param nearXidVer Transaction id.
     * @param writeVer Transaction entries write topology version.
     * @param participatingNodes Primary -> Backup nodes participating in transaction.
     * @param ts TimeStamp.
     */
    public TxRecord(
        TransactionState state,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        @Nullable Map<Short, Collection<Short>> participatingNodes,
        long ts
    ) {
        super(ts);

        this.state = state;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.participatingNodes = participatingNodes;
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
     * @return DHT version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Transaction state.
     */
    public TransactionState state() {
        return state;
    }

    /**
     * @return Primary -> backup participating nodes compact IDs.
     */
    public Map<Short, Collection<Short>> participatingNodes() {
        return participatingNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxRecord.class, this, "super", super.toString());
    }
}
