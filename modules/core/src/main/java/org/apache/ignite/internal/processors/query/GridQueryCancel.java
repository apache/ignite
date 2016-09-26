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
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Contains cancellation closure.
 */
public class GridQueryCancel {
    /**
     * No op. static closure. Used for representing cancelled state.
     */
    private static final Runnable NO_OP = new Runnable() {
        @Override
        public void run() {
            // No-op.
        }
    };

    /**
     * Cancel requested.
     */
    private volatile boolean cancelled;

    /**
     * Future.
     */
    private final GridFutureAdapter<Runnable> fut = new GridFutureAdapter<>();

    /**
     * @param clo Clo.
     */
    public void set(Runnable clo) throws QueryCancelledException{
        checkCancelled();

        fut.onDone(clo);
    }

    /**
     * Tries to cancel a query. Only one thread can call this method. This is enforced by QueryCursorImpl.
     */
    public void cancel() {
        cancelled = true;

        try {
            Runnable clo0 = fut.get();

            clo0.run();
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }


    /**
     * Check cancel state.
     */
    public void checkCancelled() throws QueryCancelledException{
        if (cancelled)
            throw new QueryCancelledException();
    }

    /**
     * @return Cancel future. Used by queries to notify about entering cancellable state.
     */
    public GridFutureAdapter<Runnable> future() {
        return fut;
    }
}