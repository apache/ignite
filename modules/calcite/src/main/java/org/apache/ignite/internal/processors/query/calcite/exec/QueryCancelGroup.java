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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryCancellable;

/** */
public final class QueryCancelGroup implements QueryCancellable {
    /** */
    private final FailureProcessor failureProcessor;

    /** */
    private final Set<QueryCancellable> queries;

    /** */
    private boolean cancelled;

    /** */
    public QueryCancelGroup(GridQueryCancel cancel, FailureProcessor failureProcessor) {
        this.failureProcessor = failureProcessor;

        queries = new HashSet<>();

        register(cancel);
    }

    /**
     * Adds a cancellable to the group.
     *
     * @param query Query cancellable object.
     * @return {@code false} if query was cancelled before this call.
     */
    public synchronized boolean add(QueryCancellable query) {
        if (cancelled)
            return false;

        boolean res = queries.add(query);

        assert res;

        return true;
    }

    /** {@inheritDoc} */
    @Override public synchronized void doCancel() {
        cancelled = true;

        try {
            for (QueryCancellable query : queries)
                query.doCancel();
        }
        catch (Exception e) {
            failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** */
    private void register(GridQueryCancel cancel) {
        try {
            cancel.set(this);
        }
        catch (QueryCancelledException e) {
            throw new IgniteSQLException(QueryCancelledException.ERR_MSG, IgniteQueryErrorCode.QUERY_CANCELED);
        }
    }
}
