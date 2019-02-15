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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Local transaction API.
 */
public interface IgniteTxLocalEx extends IgniteInternalTx {
    /**
     * @return Minimum version involved in transaction.
     */
    public GridCacheVersion minVersion();

    /**
     * @return Commit error.
     */
    @Nullable public Throwable commitError();

    /**
     * @throws IgniteCheckedException If commit failed.
     */
    public void userCommit() throws IgniteCheckedException;

    /**
     * @param clearThreadMap If {@code true} removes {@link GridNearTxLocal} from thread map.
     * @throws IgniteCheckedException If rollback failed.
     */
    public void userRollback(boolean clearThreadMap) throws IgniteCheckedException;

    /**
     * Finishes transaction (either commit or rollback).
     *
     * @param commit {@code True} if commit, {@code false} if rollback.
     * @param clearThreadMap If {@code true} removes {@link GridNearTxLocal} from thread map.
     * @return {@code True} if state has been changed.
     * @throws IgniteCheckedException If finish failed.
     */
    public boolean localFinish(boolean commit, boolean clearThreadMap) throws IgniteCheckedException;

    /**
     * Remembers that particular cache partition was touched by current tx.
     *
     * @param cacheId Cache id.
     * @param partId Partition id.
     */
    public void touchPartition(int cacheId, int partId);
}
