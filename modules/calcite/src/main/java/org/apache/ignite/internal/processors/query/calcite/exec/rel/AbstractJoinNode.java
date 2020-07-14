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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;

/** */
public abstract class AbstractJoinNode<Row> extends AbstractNode<Row> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    /** */
    protected final Predicate<Row> cond;

    /** */
    protected final RowHandler<Row> handler;

    /** */
    protected int requested;

    /** */
    protected int waitingLeft;

    /** */
    protected int waitingRight;

    /** */
    protected final List<Row> rightMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /** */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    protected AbstractJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond) {
        super(ctx);

        this.cond = cond;
        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 2;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::body);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx == 0)
            return new Downstream<Row>() {
                /** {@inheritDoc} */
                @Override public void push(Row row) {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override public void end() {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override public void onError(Throwable e) {
                    AbstractJoinNode.this.onError(e);
                }
            };
        else if (idx == 1)
            return new Downstream<Row>() {
                /** {@inheritDoc} */
                @Override public void push(Row row) {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override public void end() {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override public void onError(Throwable e) {
                    AbstractJoinNode.this.onError(e);
                }
            };

        throw new IndexOutOfBoundsException();
    }

    /** */
    private void pushLeft(Row row) {
        checkThread();

        assert downstream != null;
        assert waitingLeft > 0;

        waitingLeft--;

        leftInBuf.add(row);

        body();
    }

    /** */
    private void pushRight(Row row) {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        waitingRight--;

        rightMaterialized.add(row);

        if (waitingRight == 0)
            sources.get(1).request(waitingRight = IN_BUFFER_SIZE);
    }

    /** */
    private void endLeft() {
        checkThread();

        assert downstream != null;
        assert waitingLeft > 0;

        waitingLeft = NOT_WAITING;

        body();
    }

    /** */
    private void endRight() {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        waitingRight = NOT_WAITING;

        body();
    }

    /** */
    private void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** */
    private void body() {
        inLoop = true;
        try {
            doJoin();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
        finally {
            inLoop = false;
        }
    }

    /** */
    protected abstract void doJoin();
}
