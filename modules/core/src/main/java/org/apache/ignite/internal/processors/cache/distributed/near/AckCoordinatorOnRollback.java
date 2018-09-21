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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.CIX1;

/** */
public class AckCoordinatorOnRollback extends CIX1<IgniteInternalFuture<IgniteInternalTx>> {
    /** */
    private static final long serialVersionUID = 8172699207968328284L;

    /** */
    private final GridNearTxLocal tx;

    /**
     * @param tx Transaction.
     */
    public AckCoordinatorOnRollback(GridNearTxLocal tx) {
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public void applyx(IgniteInternalFuture<IgniteInternalTx> fut) throws IgniteCheckedException {
        assert fut.isDone();

        MvccQueryTracker tracker = tx.mvccQueryTracker();
        MvccSnapshot mvccSnapshot = tx.mvccSnapshot();

        if (tracker != null) // Optimistic tx.
            tracker.onDone(tx, false);
        else if (mvccSnapshot != null)// Pessimistic tx.
            tx.context().coordinators().ackTxRollback(mvccSnapshot);
    }
}
