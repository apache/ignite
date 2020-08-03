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
 *
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.RunningQueryManager;

/**
 * Query cursor for registered as running queries.
 *
 * Running query will be unregistered during close of cursor.
 */
public class RegisteredQueryCursor<T> extends QueryCursorImpl<T> {
    /** */
    private final AtomicBoolean unregistered = new AtomicBoolean(false);

    /** */
    private RunningQueryManager runningQryMgr;

    /** */
    private Long qryId;

    /** Exception caused query failed or {@code null} if it succeded. */
    private Exception failReason;

    /**
     * @param iterExec Query executor.
     * @param cancel Cancellation closure.
     * @param runningQryMgr Running query manager.
     * @param lazy Lazy mode flag.
     * @param qryId Registered running query id.
     */
    public RegisteredQueryCursor(Iterable<T> iterExec, GridQueryCancel cancel, RunningQueryManager runningQryMgr,
        boolean lazy, Long qryId) {
        super(iterExec, cancel, true, lazy);

        assert runningQryMgr != null;
        assert qryId != null;

        this.runningQryMgr = runningQryMgr;
        this.qryId = qryId;
    }

    /** {@inheritDoc} */
    @Override protected Iterator<T> iter() {
        try {
            if (lazy())
                return new RegisteredIterator(super.iter());
            else
                return super.iter();
        }
        catch (Exception e) {
            failReason = e;

            if (QueryUtils.wasCancelled(failReason))
                unregisterQuery();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        super.close();

        unregisterQuery();
    }

    /**
     * Cancels query.
     */
    public void cancel() {
        if (failReason == null)
            failReason = new QueryCancelledException();

        close();
    }

    /**
     * Unregister query.
     */
    private void unregisterQuery() {
        if (unregistered.compareAndSet(false, true))
            runningQryMgr.unregister(qryId, failReason);
    }

    /**
     *
     */
    private class RegisteredIterator implements Iterator<T> {
        /** Delegate iterator. */
        final Iterator<T> delegateIt;

        /**
         * @param it Result set iterator.
         */
        private RegisteredIterator(Iterator<T> it) {
            delegateIt = it;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            try {
                return delegateIt.hasNext();
            }
            catch (Exception e) {
                failReason = e;

                if (QueryUtils.wasCancelled(failReason))
                    unregisterQuery();

                throw e;
            }
        }

        /** {@inheritDoc} */
        @Override public T next() {
            try {
                return delegateIt.next();
            }
            catch (Exception e) {
                failReason = e;

                if (QueryUtils.wasCancelled(failReason))
                    unregisterQuery();

                throw e;
            }
        }
    }
}
