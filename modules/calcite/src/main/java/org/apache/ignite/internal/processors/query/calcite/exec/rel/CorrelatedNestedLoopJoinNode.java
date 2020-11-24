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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
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
    private State state = State.INITIAL;

    /** */
    private enum State {
        INITIAL, FILLING_LEFT, FILLING_RIGHT, IDLE, IN_LOOP, END
    }

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public CorrelatedNestedLoopJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, Predicate<Row> cond, Set<CorrelationId> correlationIds) {
        super(ctx, rowType);

        assert !F.isEmpty(correlationIds);

        this.cond = cond;
        this.correlationIds = new ArrayList<>(correlationIds);

        leftInBufferSize = correlationIds.size();
        rightInBufferSize = IN_BUFFER_SIZE;

        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        try {
            checkState();

            requested = rowsCnt;

            onRequest();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        leftInBuf = null;
        rightInBuf = null;

        leftIdx = 0;
        rightIdx = 0;

        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        state = State.INITIAL;
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
        assert downstream() != null;
        assert waitingLeft > 0;

        try {
            checkState();

            waitingLeft--;

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
        assert downstream() != null;
        assert waitingRight > 0;

        try {
            checkState();

            waitingRight--;

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
        assert downstream() != null;
        assert waitingLeft > 0;

        try {
            checkState();

            waitingLeft = -1;

            if (leftInBuf == null)
                leftInBuf = Collections.emptyList();

            onEndLeft();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void endRight() {
        assert downstream() != null;
        assert waitingRight > 0;

        try {
            checkState();

            waitingRight = -1;

            if (rightInBuf == null)
                rightInBuf = Collections.emptyList();

            onEndRight();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void onRequest() {
        switch (state) {
            case IN_LOOP:
            case FILLING_RIGHT:
            case FILLING_LEFT:
                break;
            case INITIAL:
                assert waitingLeft == 0;
                assert waitingRight == 0;
                assert F.isEmpty(leftInBuf);
                assert F.isEmpty(rightInBuf);

                context().execute(() -> {
                    try {
                        checkState();

                        state = State.FILLING_LEFT;
                        leftSource().request(waitingLeft = leftInBufferSize);
                    }
                    catch (Exception e) {
                        onError(e);
                    }
                });

                break;
            case IDLE:
                assert rightInBuf != null;
                assert leftInBuf != null;
                assert waitingRight == -1 || waitingRight == 0 && rightInBuf.size() == rightInBufferSize;
                assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

                context().execute(() -> {
                    try {
                        checkState();

                        join();
                    }
                    catch (Exception e) {
                        onError(e);
                    }
                });

                break;

            case END:
                downstream().end();
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
                rightSource().rewind();

            state = State.FILLING_RIGHT;
            rightSource().request(waitingRight = rightInBufferSize);
        }
    }

    /** */
    private void onPushRight() throws IgniteCheckedException {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert !F.isEmpty(leftInBuf);
        assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        if (rightInBuf.size() == rightInBufferSize) {
            assert waitingRight == 0;

            state = State.IDLE;

            join();
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

            if (requested > 0)
                downstream().end();
        }
        else {
            prepareCorrelations();

            if (waitingRight == -1)
                rightSource().rewind();

            state = State.FILLING_RIGHT;

            rightSource().request(waitingRight = rightInBufferSize);
        }
    }

    /** */
    private void onEndRight() throws IgniteCheckedException {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert waitingRight == -1;
        assert !F.isEmpty(leftInBuf);
        assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        state = State.IDLE;

        join();
    }

    /** */
    private void join() throws IgniteCheckedException {
        assert state == State.IDLE;

        state = State.IN_LOOP;

        try {
            while (requested > 0 && rightIdx < rightInBuf.size()) {
                if (leftIdx == leftInBuf.size())
                    leftIdx = 0;

                while (requested > 0 && leftIdx < leftInBuf.size()) {
                    checkState();

                    Row row = handler.concat(leftInBuf.get(leftIdx), rightInBuf.get(rightIdx));

                    leftIdx++;

                    if (cond.test(row)) {
                        requested--;

                        downstream().push(row);
                    }
                }

                if (leftIdx == leftInBuf.size())
                    rightInBuf.set(rightIdx++, null);
            }
        }
        finally {
            state = State.IDLE;
        }

        if (rightIdx == rightInBuf.size()) {
            leftIdx = 0;
            rightIdx = 0;

            if (waitingRight == 0) {
                rightInBuf = null;

                state = State.FILLING_RIGHT;

                rightSource().request(waitingRight = rightInBufferSize);

                return;
            }

            if (waitingLeft == 0) {
                rightInBuf = null;
                leftInBuf = null;

                state = State.FILLING_LEFT;

                leftSource().request(waitingLeft = leftInBufferSize);

                return;
            }

            assert waitingLeft == -1 && waitingRight == -1;

            if (requested > 0) {
                leftInBuf = null;
                rightInBuf = null;

                state = State.END;

                if (requested > 0)
                    downstream().end();

                return;
            }

            // let's free the rows for GC
            leftInBuf = Collections.emptyList();
            rightInBuf = Collections.emptyList();
        }
    }

    /** */
    private Node<Row> leftSource() {
        return sources().get(0);
    }

    /** */
    private Node<Row> rightSource() {
        return sources().get(1);
    }

    private void prepareCorrelations() {
        for (int i = 0; i < correlationIds.size(); i++) {
            Row row = i < leftInBuf.size() ? leftInBuf.get(i) : F.first(leftInBuf);
            context().setCorrelated(row, correlationIds.get(i).getId());
        }
    }
}
