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
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionState;

/**
 *
 */
public class GridNearTxFinishAndAckFuture extends GridFutureAdapter<IgniteInternalTx> implements NearTxFinishFuture {
    /** */
    private final NearTxFinishFuture finishFut;

    /**
     * @param finishFut Finish future.
     */
    GridNearTxFinishAndAckFuture(NearTxFinishFuture finishFut) {
        finishFut.listen(this::onFinishFutureDone);

        this.finishFut = finishFut;
    }

    /** {@inheritDoc} */
    @Override public boolean commit() {
        return finishFut.commit();
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal tx() {
        return finishFut.tx();
    }

    /** {@inheritDoc} */
    @Override public void onNodeStop(IgniteCheckedException e) {
        finishFut.onNodeStop(e);
    }

    /** {@inheritDoc} */
    @Override public void finish(boolean commit, boolean clearThreadMap, boolean onTimeout) {
        finishFut.finish(commit, clearThreadMap, onTimeout);
    }

    /** */
    private void onFinishFutureDone(IgniteInternalFuture<IgniteInternalTx> fut) {
        GridNearTxLocal tx = tx(); Throwable err = fut.error();

        if (tx.state() == TransactionState.COMMITTED)
            tx.context().coordinators().ackTxCommit(tx.mvccSnapshot())
                .listen(fut0 -> onDone(tx, addSuppressed(err, fut0.error())));
        else {
            tx.context().coordinators().ackTxRollback(tx.mvccSnapshot());

            onDone(tx, err);
        }
    }

    /** */
    private Throwable addSuppressed(Throwable to, Throwable ex) {
        if (ex == null)
            return to;
        else if (to == null)
            return ex;
        else
            to.addSuppressed(ex);

        return to;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxFinishAndAckFuture.class, this);
    }
}
