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
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Logical data record indented for MVCC transaction related actions.<br>
 * This record is marker of prepare, commit, and rollback transactions.
 */
public class MvccTxRecord extends TxRecord implements WalRecordCacheGroupAware {
    /** Transaction mvcc snapshot version. */
    private final MvccVersion mvccVer;

    /**
     * @param state Transaction state.
     * @param nearXidVer Transaction id.
     * @param writeVer Transaction entries write topology version.
     * @param participatingNodes Primary -> Backup nodes compact IDs participating in transaction.
     * @param mvccVer Transaction snapshot version.
     */
    public MvccTxRecord(
        TransactionState state,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        @Nullable Map<Short, Collection<Short>> participatingNodes,
        MvccVersion mvccVer
    ) {
        super(state, nearXidVer, writeVer, participatingNodes);

        this.mvccVer = mvccVer;
    }

    /**
     * @param state Transaction state.
     * @param nearXidVer Transaction id.
     * @param writeVer Transaction entries write topology version.
     * @param mvccVer Transaction snapshot version.
     * @param participatingNodes Primary -> Backup nodes participating in transaction.
     * @param ts TimeStamp.
     */
    public MvccTxRecord(
        TransactionState state,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        @Nullable Map<Short, Collection<Short>> participatingNodes,
        MvccVersion mvccVer,
        long ts
    ) {
        super(state, nearXidVer, writeVer, participatingNodes, ts);

        this.mvccVer = mvccVer;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.MVCC_TX_RECORD;
    }

    /**
     * @return Mvcc version.
     */
    public MvccVersion mvccVersion() {
        return mvccVer;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return TxLog.TX_LOG_CACHE_ID;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccTxRecord.class, this, "super", super.toString());
    }
}