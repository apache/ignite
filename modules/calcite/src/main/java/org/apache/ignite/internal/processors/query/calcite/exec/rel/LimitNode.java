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

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class LimitNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private Supplier<CompletableFuture<Integer>> offsetSup;

    private long offset = -1;

    /** */
    private Supplier<CompletableFuture<Integer>> fetchSup;

    /** */
    private long fetch = -1;

    /** */
    private long requested;

    /** */
    private int waiting;

    /** */
    private boolean inLoop;

    /** */
    private int processed;

    /** */
    private CompletableFuture<Void> futLimitsReady;

    /**
     * @param ctx Execution context.
     */
    public LimitNode(
        ExecutionContext<Row> ctx,
        Supplier<CompletableFuture<Integer>> offsetSup,
        Supplier<CompletableFuture<Integer>> fetchSup) {
        super(ctx);

        this.offsetSup = offsetSup;
        this.fetchSup = fetchSup;

        if (offsetSup == null)
            offset = 0;

        if (fetchSup == null)
            fetch = 0;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCnt > 0;

        requested += rowsCnt;

        if (futLimitsReady == null) {
            CompletableFuture<Integer> offFut = null;
            CompletableFuture<Integer> fetchFut = null;

            if (offset < 0 && offsetSup != null) {
                offFut = offsetSup.get();

                offFut.thenAccept(n -> offset = n);
            }

            if (fetch < 0 && fetchSup != null) {
                fetchFut = offsetSup.get();

                fetchFut.thenAccept(n -> fetch = n);
            }

            if (offFut != null && fetchFut != null)
                futLimitsReady = CompletableFuture.allOf(offFut, fetchFut);
            else if (offFut != null)
                futLimitsReady = CompletableFuture.allOf(offFut);
            else {
                assert fetchFut != null;

                futLimitsReady = CompletableFuture.allOf(fetchFut);
            }

            futLimitsReady.thenAccept((v) -> {
                System.out.println(Thread.currentThread().getName() + " +++ OFF/FETCH DONE: off=" + offset + ", fetch=" + fetch);

                fetch += offset;

                requestAfterLimitsReady();
            });
        }
        else {
            if (futLimitsReady.isDone())
                requestAfterLimitsReady();
        }
    }

    /**
     * Requests next bunch of rows when offset / fetched fave been calculated.
     */
    private void requestAfterLimitsReady() {
        System.out.println(Thread.currentThread().getName() + "+++ req: " + Math.min(fetch, requested - processed));
        F.first(sources).request((int)Math.min(fetch, requested - processed));
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        checkThread();

        assert downstream != null;

        if (processed > offset)
            downstream.push(row);

        processed++;

        if (processed > fetch)
            downstream.end();
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
