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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.ResultSet;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryResultsEnlistFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class NearResultSetEnlistFuture extends GridNearTxQueryResultsEnlistFuture implements ResultSetEnlistFuture  {
    /**
     * @param nearNodeId   Near node ID.
     * @param nearLockVer  Near lock version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId     Thread ID.
     * @param nearFutId    Near future id.
     * @param nearMiniId   Near mini future id.
     * @param parts        Partitions.
     * @param tx           Transaction.
     * @param timeout      Lock acquisition timeout.
     * @param cctx         Cache context.
     * @param rs           Result set to process.
     */
    public NearResultSetEnlistFuture(UUID nearNodeId, GridCacheVersion nearLockVer, MvccSnapshot mvccSnapshot,
        long threadId, IgniteUuid nearFutId, int nearMiniId, @Nullable int[] parts, GridDhtTxLocalAdapter tx,
        long timeout, GridCacheContext<?, ?> cctx, ResultSet rs) {
        super(cctx, (GridNearTxLocal)tx, timeout, ResultSetEnlistFuture.createIterator(rs), 0, true);
    }


    /** {@inheritDoc} */
    @Override public void onError(Throwable error) {
        onDone(error);
    }
}
