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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.cache.query.QueryCancelledException;

import static org.apache.ignite.internal.processors.query.GridQueryCancel.State.CANCEL_REQUESTED;
import static org.apache.ignite.internal.processors.query.GridQueryCancel.State.CANCELLABLE;

/**
 * Holds query cancel state.
 */
public class GridQueryCancel {
    /** */
    private final static AtomicReferenceFieldUpdater<GridQueryCancel, State> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(GridQueryCancel.class, State.class, "state");

    /** */
    private volatile State state = null;

    /** */
    private volatile Runnable clo;

    /**
     * Sets a cancel closure.
     *
     * @param clo Clo.
     */
    public void set(Runnable clo) throws QueryCancelledException {
        assert clo != null;

        this.clo = clo;

        if (!STATE_UPDATER.compareAndSet(this, null, CANCELLABLE))
            throw new QueryCancelledException();
    }

    /**
     * Executes cancel closure if a query is in appropriate state.
     */
    public void cancel() {
        if (!STATE_UPDATER.compareAndSet(this, null, CANCEL_REQUESTED))
            clo.run();
    }

    /**
     * Stops query execution if a user requested cancel.
     */
    public void checkCancelled() throws QueryCancelledException{
        if (state==CANCEL_REQUESTED)
            throw new QueryCancelledException();
    }

    /** Query cancel state */
    protected enum State {
        /** Cancel requested. */CANCEL_REQUESTED,
        /** Query in cancellable state. */CANCELLABLE,
    }
}