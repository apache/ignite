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
 * Contains cancellation closure.
 */
public class GridQueryCancel {
    /**
     * Cancel requested state.
     */
    private volatile boolean cancelled;

    /**
     * Query completed state.
     */
    private volatile boolean completed;

    /**
     * Cancel closure.
     */
    private volatile Runnable clo;

    /**
     * @param clo Clo.
     */
    public void set(Runnable clo) throws QueryCancelledException{
        checkCancelled();

        this.clo = clo;
    }

    /**
     * Spins until a query is completed by cancel or normal termination.
     * Only one thread can enter this method.
     * This is guaranteed by {@link org.apache.ignite.internal.processors.cache.QueryCursorImpl}
     */
    public void cancel() {
        cancelled = true;

        int attempt = 0;
        do {
            int sleep = attempt++ * 10;

            if (sleep != 0)
                try {
                    U.sleep(sleep);
                } catch (IgniteInterruptedCheckedException ignored) {
                    return;
                }

            if (clo != null) clo.run();
        } while(!completed);
    }

    /**
     * Check cancel state.
     */
    public void checkCancelled() throws QueryCancelledException{
        if (cancelled)
            throw new QueryCancelledException();
    }

    /**
     * Signals the spinner to stop because two things have been happened: query was completed or cancelled.
     */
    public void done() {
        completed = true;
    }
}