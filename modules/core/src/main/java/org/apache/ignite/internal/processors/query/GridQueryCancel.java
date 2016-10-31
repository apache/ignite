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

import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Holds query cancel state.
 */
public class GridQueryCancel {
    /** */
    private volatile boolean cancelled;

    /** */
    private volatile boolean completed;

    /** */
    private volatile Runnable clo;

    /**
     * Sets a cancel closure. The closure must be idempotent to multiple invocations.
     *
     * @param clo Clo.
     */
    public void set(Runnable clo) throws QueryCancelledException{
        checkCancelled();

        this.clo = clo;
    }

    /**
     * Spins until a query is completed.
     * Only one thread can enter this method.
     * This is guaranteed by {@link org.apache.ignite.internal.processors.cache.QueryCursorImpl}
     */
    public void cancel() {
        cancelled = true;

        int attempt = 0;

        while (!completed) {
            if (clo != null) clo.run();

            try {
                U.sleep(++attempt * 10);
            } catch (IgniteInterruptedCheckedException ignored) {
                return;
            }
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
        completed = true;
    }
}