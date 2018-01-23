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
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 *
 */
public class GridNearTxFinishAndAckFuture extends GridFutureAdapter<IgniteInternalTx> implements NearTxFinishFuture {
    /** */
    private final GridNearTxFinishFuture finishFut;

    /**
     * @param finishFut Finish future.
     */
    GridNearTxFinishAndAckFuture(GridNearTxFinishFuture finishFut) {
        this.finishFut = finishFut;
    }

    /** {@inheritDoc} */
    @Override public boolean commit() {
        return finishFut.commit();
    }

    /** {@inheritDoc} */
    public void finish(boolean commit, boolean clearThreadMap) {
        if (commit) {
            finishFut.finish(true, clearThreadMap);

            finishFut.listen(new IgniteInClosure<GridNearTxFinishFuture>() {
                @Override public void apply(final GridNearTxFinishFuture fut) {
                    GridNearTxLocal tx = fut.tx();

                    IgniteInternalFuture<Void> ackFut = null;

                    MvccQueryTracker qryTracker = tx.mvccQueryTracker();

                    MvccTxInfo mvccInfo = tx.mvccInfo();

                    if (qryTracker != null)
                        ackFut = qryTracker.onTxDone(mvccInfo, fut.context(), true);
                    else if (mvccInfo != null) {
                        ackFut = fut.context().coordinators().ackTxCommit(mvccInfo.coordinatorNodeId(),
                            mvccInfo.version(),
                            null);
                    }

                    if (ackFut != null) {
                        ackFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                            @Override public void apply(IgniteInternalFuture<Void> ackFut) {
                                Exception err = null;

                                try {
                                    fut.get();

                                    ackFut.get();
                                }
                                catch (Exception e) {
                                    err = e;
                                }
                                catch (Error e) {
                                    onDone(e);

                                    throw e;
                                }

                                if (err != null)
                                    onDone(err);
                                else
                                    onDone(fut.tx());
                            }
                        });
                    }
                    else
                        finishWithFutureResult(fut);
                }
            });
        }
        else {
            finishFut.finish(false, clearThreadMap);

            finishFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    finishWithFutureResult(fut);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeStop(IgniteCheckedException e) {
        super.onDone(finishFut.tx(), e);
    }

    /**
     * @param fut Future.
     */
    private void finishWithFutureResult(IgniteInternalFuture<IgniteInternalTx> fut) {
        try {
            onDone(fut.get());
        }
        catch (IgniteCheckedException | RuntimeException e) {
            onDone(e);
        }
        catch (Error e) {
            onDone(e);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxFinishAndAckFuture.class, this);
    }
}
