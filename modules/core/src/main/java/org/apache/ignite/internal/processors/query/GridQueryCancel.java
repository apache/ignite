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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Holds query cancel state.
 */
public class GridQueryCancel {
    /** */
    private static final Runnable EMPTY = new Runnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** */
    private volatile boolean cancelled;

    /** */
    private final GridFutureAdapter<Runnable> cancelFut = new GridFutureAdapter<>();

    /**
     * Sets a cancel closure. The closure must be idempotent to multiple invocations.
     *
     * @param clo Clo.
     */
    public void set(Runnable clo) throws QueryCancelledException{
        assert clo != null;

        checkCancelled();

        cancelFut.onDone(clo);
    }

    /**
     * Waits until query will enter cancellable state and executes cancel closure.
     */
    public void cancel() {
        cancelled = true;

        try {
            cancelFut.get().run();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Stops query execution if a user requested cancel.
     */
    public void checkCancelled() throws QueryCancelledException{
        if (cancelled)
            throw new QueryCancelledException();
    }

    /**
     * Sets completed state.
     * The method must be called then a query is completed by any reason, typically in final block.
     */
    public void setCompleted() {
        cancelFut.onDone(EMPTY);
    }
}