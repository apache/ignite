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

package org.apache.ignite.internal.util.future;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Future listener to fill chained future with converted result of the source future.
 */
class GridFutureChainListener<T, R> implements IgniteInClosure<IgniteInternalFuture<T>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Target future. */
    private final GridFutureAdapter<R> fut;

    /** Done callback. */
    private final IgniteClosure<? super IgniteInternalFuture<T>, R> doneCb;

    /** */
    private Executor cbExec;

    /**
     * Constructs chain listener.
     *
     *  @param fut Target future.
     * @param doneCb Done callback.
     * @param cbExec Optional executor to run callback.
     */
    public GridFutureChainListener(
        GridFutureAdapter<R> fut,
        IgniteClosure<? super IgniteInternalFuture<T>, R> doneCb,
        @Nullable Executor cbExec
    ) {
        this.fut = fut;
        this.doneCb = doneCb;
        this.cbExec = cbExec;
    }

    /** {@inheritDoc} */
    @Override public void apply(final IgniteInternalFuture<T> t) {
        if (cbExec != null) {
            cbExec.execute(new Runnable() {
                @Override public void run() {
                    applyCallback(t);
                }
            });
        }
        else
            applyCallback(t);
    }

    /**
     * @param t Target future.
     */
    private void applyCallback(IgniteInternalFuture<T> t) {
        try {
            fut.onDone(doneCb.apply(t));
        }
        catch (GridClosureException e) {
            fut.onDone(e.unwrap());
        }
        catch (RuntimeException | Error e) {
            fut.onDone(e);

            throw e;
        }
    }
}