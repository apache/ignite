/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Iterates over results of TX DML reducer.
 */
class TxDmlReducerIterator extends GridCloseableIteratorAdapterEx<IgniteBiTuple> {
    /** */
    private final UpdatePlan plan;

    /** */
    private final QueryCursor<List<?>> cur;

    /** */
    private final Iterator<List<?>> it;

    /**
     *
     * @param plan Update plan.
     * @param cur Cursor.
     */
    TxDmlReducerIterator(UpdatePlan plan, QueryCursor<List<?>> cur) {
        this.plan = plan;
        this.cur = cur;

        it = cur.iterator();
    }

    /** {@inheritDoc} **/
    @Override protected IgniteBiTuple onNext() throws IgniteCheckedException {
        return plan.processRowForTx(it.next());
    }

    /** {@inheritDoc} **/
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return it.hasNext();
    }

    /** {@inheritDoc} **/
    @Override protected void onClose() throws IgniteCheckedException {
        cur.close();
    }
}
