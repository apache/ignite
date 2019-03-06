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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Future which waits for completion of one or more transactions.
 */
public final class GridCacheMultiTxFuture<K, V> extends GridFutureAdapter<Boolean> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** */
    private Set<IgniteInternalTx> remainingTxs;

    /**
     * @param cctx Cache context.
     */
    public GridCacheMultiTxFuture(GridCacheContext<K, V> cctx) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMultiTxFuture.class);
    }

    /**
     * @param tx Transaction to add.
     */
    public void addTx(IgniteInternalTx tx) {
        if (remainingTxs == null)
            remainingTxs = new GridConcurrentHashSet<>();

        remainingTxs.add(tx);
    }

    /**
     * Initializes this future.
     */
    public void init() {
        if (remainingTxs == null) {
            remainingTxs = Collections.emptySet();

            onDone(true);
        }
        else {
            assert !remainingTxs.isEmpty();

            for (final IgniteInternalTx tx : remainingTxs) {
                if (!tx.done()) {
                    tx.finishFuture().listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                        @Override public void apply(IgniteInternalFuture<IgniteInternalTx> t) {
                            remainingTxs.remove(tx);

                            checkRemaining();
                        }
                    });
                }
                else
                    remainingTxs.remove(tx);
            }

            checkRemaining();
        }
    }

    /**
     * @return {@code True} if remaining set is empty.
     */
    private boolean checkRemaining() {
        if (remainingTxs.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Finishing multi-tx future: " + this);

            onDone(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMultiTxFuture.class, this,
            "remaining", F.viewReadOnly(remainingTxs, CU.<K, V>tx2xidVersion()),
            "super", super.toString()
        );
    }
}