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
    private static final Cancellable CANCELLED = new Cancellable() {

        @Override public String buildExceptionMessage() {
            return null; //checked
        }

        @Override public void run() {
            // No-op.
        }
    };

    /** */
    private static final AtomicReferenceFieldUpdater<GridQueryCancel, Cancellable> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(GridQueryCancel.class, Cancellable.class, "clo");

    /** */
    private volatile Cancellable clo;

    /** */
    private volatile String msg;

    /**
     * Sets a cancel closure.
     *
     * @param clo Clo.
     */
    public void set(Cancellable clo) throws QueryCancelledException {
        assert clo != null;

        while(true) {
            Cancellable tmp = this.clo;

            if (tmp == CANCELLED)
                throw new QueryCancelledException(msg != null ? msg : clo.buildExceptionMessage());

            if (STATE_UPDATER.compareAndSet(this, tmp, clo))
                return;
        }
    }

    /**
     * Executes cancel closure.
     */
    public void cancel() {
        while(true) {
            Cancellable tmp = this.clo;

            if (tmp != null) {
                String msg = tmp.buildExceptionMessage();

                //CANCELLED state has null
                if (msg != null)
                    this.msg = msg;
            }

            if (STATE_UPDATER.compareAndSet(this, tmp, CANCELLED)) {
                if (tmp != null)
                    tmp.run();

                return;
            }
        }
    }

    /**
     * Stops query execution if a user requested cancel.
     */
    public void checkCancelled() throws QueryCancelledException {
        if (clo == CANCELLED)
            throw new QueryCancelledException(msg != null ? msg : "The query was cancelled before initialization.");
    }

    /**
     * Special interface for closure
     */
    public interface Cancellable extends Runnable {
        /**
         * @return Message to show in case of exception
         */
        String buildExceptionMessage();
    }
}
