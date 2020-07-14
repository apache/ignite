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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.calcite.rel.core.CorrelationId;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class CorrelatedNestedLoopJoinNode<Row> extends AbstractNode<Row> {
    /** */
    private final Predicate<Row> cond;

    /** */
    private final List<CorrelationId> correlationIds;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final int leftInBufferSize;

    /** */
    private final int rightInBufferSize;

    /** */
    private int requested;

    /** */
    private int waitingLeft;

    /** */
    private int waitingRight;

    /** */
    private List<Row> leftInBuf;

    /** */
    private List<Row> rightInBuf;

    /** */
    private int leftIdx;

    /** */
    private int rightIdx;

    /** */
    private State state = State.NEW;

    /** */
    private enum State {
        NEW, FILLING_LEFT, FILLING_RIGHT, READY, IN_LOOP, END
    }

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public CorrelatedNestedLoopJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond, Set<CorrelationId> correlationIds) {
        super(ctx);

        assert !F.isEmpty(correlationIds);

        this.cond = cond;
        this.correlationIds = new ArrayList<>(correlationIds);

        leftInBufferSize = correlationIds.size();
        rightInBufferSize = IN_BUFFER_SIZE;

        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 2;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        onRequest();
    }

    /** {@inheritDoc} */
    @Override protected void resetInternal() {
        leftInBuf = null;
        rightInBuf = null;

        leftIdx = 0;
        rightIdx = 0;

        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        state = State.NEW;
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
                    CorrelatedNestedLoopJoinNode.this.onError(e);
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
                    CorrelatedNestedLoopJoinNode.this.onError(e);
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

        try {
            if (leftInBuf == null)
                leftInBuf = new ArrayList<>(leftInBufferSize);

            leftInBuf.add(row);

            onPushLeft();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void pushRight(Row row) {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        waitingRight--;

        try {
            if (rightInBuf == null)
                rightInBuf = new ArrayList<>(rightInBufferSize);

            rightInBuf.add(row);

            onPushRight();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void endLeft() {
        checkThread();

        assert downstream != null;
        assert waitingLeft > 0;

        try {
            if (leftInBuf == null)
                leftInBuf = Collections.emptyList();

            waitingLeft = -1;

            onEndLeft();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void endRight() {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        try {
            if (rightInBuf == null)
                rightInBuf = Collections.emptyList();;

            waitingRight = -1;

            onEndRight();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** */
    private void onRequest() {
        switch (state) {
            case IN_LOOP:
                break;
            case NEW:
                assert waitingLeft == 0;
                assert waitingRight == 0;
                assert F.isEmpty(leftInBuf);
                assert F.isEmpty(rightInBuf);

                context().execute(() -> {
                    try {
                        state = State.FILLING_LEFT;
                        sources.get(0).request(waitingLeft = leftInBufferSize);
                    }
                    catch (Exception e) {
                        onError(e);
                    }
                });

                break;
            case READY:
                assert rightInBuf != null;
                assert leftInBuf != null;
                assert waitingRight == -1 || waitingRight == 0 && rightInBuf.size() == rightInBufferSize;
                assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

                context().execute(() -> {
                    try {
                        doJoin();
                    }
                    catch (Exception e) {
                        onError(e);
                    }
                });

                break;
            default:
                throw new AssertionError("Unexpected state:" + state);
        }
    }

    /** */
    private void onPushLeft() {
        assert state == State.FILLING_LEFT : "Unexpected state:" + state;
        assert waitingRight == 0 || waitingRight == -1;
        assert F.isEmpty(rightInBuf);

        if (leftInBuf.size() == leftInBufferSize) {
            assert waitingLeft == 0;

            prepareCorrelations();

            if (waitingRight == -1)
                sources.get(1).reset();

            state = State.FILLING_RIGHT;
            sources.get(1).request(waitingRight = rightInBufferSize);
        }
    }

    /** */
    private void onPushRight() {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert !F.isEmpty(leftInBuf);
        assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        if (rightInBuf.size() == rightInBufferSize) {
            assert waitingRight == 0;

            state = State.READY;

            doJoin();
        }
    }

    /** */
    private void onEndLeft() {
        assert state == State.FILLING_LEFT : "Unexpected state:" + state;
        assert waitingLeft == -1;
        assert waitingRight == 0 || waitingRight == -1;
        assert F.isEmpty(rightInBuf);

        if (F.isEmpty(leftInBuf)) {
            waitingRight = -1;
            state = State.END;

            requested = 0;
            downstream.end();
        }
        else {
            prepareCorrelations();

            if (waitingRight == -1)
                sources.get(1).reset();

            state = State.FILLING_RIGHT;
            sources.get(1).request(waitingRight = rightInBufferSize);
        }
    }

    /** */
    private void onEndRight() {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert waitingRight == -1;
        assert !F.isEmpty(leftInBuf);
        assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        state = State.READY;

        doJoin();
    }

    /** */
    private void doJoin() {
        assert state == State.READY;

        state = State.IN_LOOP;
        try {
            while (requested > 0 && rightIdx < rightInBuf.size()) {
                if (leftIdx == leftInBuf.size())
                    leftIdx = 0;

                while (requested > 0 && leftIdx < leftInBuf.size()) {
                    Row row = handler.concat(leftInBuf.get(leftIdx), rightInBuf.get(rightIdx));

                    leftIdx++;

                    if (cond.test(row)) {
                        requested--;
                        downstream.push(row);
                    }

                    // Execution may be reset or cancelled by downstream.
                    if (state != State.IN_LOOP)
                        return;
                }

                if (leftIdx == leftInBuf.size())
                    rightInBuf.set(rightIdx++, null);
            }
        }
        finally {
            if (state == State.IN_LOOP)
                state = State.READY;
        }

        if (rightIdx == rightInBuf.size()) {
            leftIdx = 0;
            rightIdx = 0;

            if (waitingRight == 0) {
                rightInBuf = null;

                state = State.FILLING_RIGHT;

                sources.get(1).request(waitingRight = rightInBufferSize);

                return;
            }

            if (waitingLeft == 0) {
                rightInBuf = null;
                leftInBuf = null;

                state = State.FILLING_LEFT;

                sources.get(0).request(waitingLeft = leftInBufferSize);

                return;
            }

            assert waitingLeft == -1 && waitingRight == -1;

            if (requested > 0) {
                leftInBuf = null;
                rightInBuf = null;

                state = State.END;

                requested = 0;
                downstream.end();

                return;
            }

            // let's free the rows for GC
            leftInBuf = Collections.emptyList();
            rightInBuf = Collections.emptyList();
        }
    }

    private void prepareCorrelations() {
        for (int i = 0; i < correlationIds.size(); i++) {
            Row row = i < leftInBuf.size() ? leftInBuf.get(i) : F.first(leftInBuf);
            context().setCorrelated(row, correlationIds.get(i).getId());
        }
    }
}
