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

/**
 * Holds query cancel state.
 */
public class GridQueryCancel {
    /** No-op runnable indicating cancelled state. */
    private static final QueryCancellable CANCELLED = () -> {
        // No-op.
    };

    /** */
    private static final AtomicReferenceFieldUpdater<GridQueryCancel, QueryCancellable> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(GridQueryCancel.class, QueryCancellable.class, "clo");

    /** */
    private volatile QueryCancellable clo;

    /**
     * Sets a cancel closure.
     *
     * @param clo Clo.
     */
    public void set(QueryCancellable clo) throws QueryCancelledException {
        assert clo != null;

        while(true) {
            QueryCancellable tmp = this.clo;

            if (tmp == CANCELLED)
                throw new QueryCancelledException();

            if (STATE_UPDATER.compareAndSet(this, tmp, clo))
                return;
        }
    }

    /**
     * Executes cancel closure.
     */
    public void cancel() {
        while(true) {
            QueryCancellable tmp = clo;

            if (STATE_UPDATER.compareAndSet(this, tmp, CANCELLED)) {
                if (tmp != null)
                    tmp.doCancel();

                return;
            }
        }
    }

    /**
     * Stops query execution if a user requested cancel.
     */
    public void checkCancelled() throws QueryCancelledException {
        if (clo == CANCELLED)
            throw new QueryCancelledException();
    }
}
