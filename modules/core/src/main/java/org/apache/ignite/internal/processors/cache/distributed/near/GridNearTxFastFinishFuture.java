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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;

/**
 *
 */
public class GridNearTxFastFinishFuture extends GridFutureAdapter<IgniteInternalTx> implements NearTxFinishFuture {
    /** */
    private final GridNearTxLocal tx;

    /** */
    private final boolean commit;

    /**
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    GridNearTxFastFinishFuture(GridNearTxLocal tx, boolean commit) {
        this.tx = tx;
        this.commit = commit;
    }

    /** {@inheritDoc} */
    @Override public boolean commit() {
        return commit;
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal tx() {
        return tx;
    }

    /**
     * @param clearThreadMap {@code True} if need remove tx from thread map.
     */
    @Override public void finish(boolean commit, boolean clearThreadMap, boolean onTimeout) {
        try {
            if (commit) {
                tx.state(PREPARING);
                tx.state(PREPARED);
                tx.state(COMMITTING);

                tx.context().tm().fastFinishTx(tx, true, true);

                tx.state(COMMITTED);
            }
            else {
                tx.state(PREPARING);
                tx.state(PREPARED);
                tx.state(ROLLING_BACK);

                tx.context().tm().fastFinishTx(tx, false, clearThreadMap);

                tx.state(ROLLED_BACK);
            }
        }
        finally {
            onDone(tx);
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeStop(IgniteCheckedException e) {
        onDone(tx, e);
    }
}
