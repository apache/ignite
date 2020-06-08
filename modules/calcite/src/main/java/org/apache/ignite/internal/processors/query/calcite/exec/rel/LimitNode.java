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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class LimitNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final static int NOT_READY = -1;

    /** */
    private final static int NOT_SET = -2;

    /** */
    final private Supplier<Integer> offsetSup;

    /** */
    final private Supplier<Integer> limitSup;

    /** */
    private long offset = NOT_READY;

    /** */
    private long limit = NOT_READY;

    /** */
    private long requested;

    /** */
    private int processed;

    /**
     * @param ctx Execution context.
     * @param offsetSup Offset parameter supplier.
     * @param limitSup Limit parameter supplier.
     */
    public LimitNode(
        ExecutionContext<Row> ctx,
        Supplier<Integer> offsetSup,
        Supplier<Integer> limitSup) {
        super(ctx);

        this.offsetSup = offsetSup;
        this.limitSup = limitSup;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCnt > 0;

        requested += rowsCnt;

        // Initialize offset / limit.
        if (offset == NOT_READY && limit == NOT_READY) {
            if (offsetSup != null) {
                offset = offsetSup.get();

                if (offset < 0)
                    onError(new IgniteSQLException("Invalid query offset: " + offset));
            }
            else
                offset = 0;

            if (limitSup != null) {
                limit = limitSup.get();

                if (limit < 0)
                    onError(new IgniteSQLException("Invalid query limit: " + limit));

                if (offset > 0)
                    limit += offset;
            }
            else
                limit = NOT_SET;
        }

        request0();
    }

    /**
     * Process request (some parameters may not yet be calculated).
     */
    private void request0() {
        if (limit == 0 || limit > 0 && processed >= limit) {
            downstream.end();

            sources.get(0).cancel();

            return;
        }

        int req = (int)(requested - processed);

        if (limit > 0)
            req = (int)Math.min(limit - processed, req);

        F.first(sources).request(req);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        checkThread();

        assert downstream != null;

        if (processed >= offset)
            downstream.push(row);

        processed++;

        if (limit > 0 && processed >= limit && processed < requested) {
            downstream.end();

            sources.get(0).cancel();
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert downstream != null;

        downstream.end();
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }
}
