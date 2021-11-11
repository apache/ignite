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
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class CorrelatedNestedLoopJoinNode<Row> extends AbstractNode<Row> {
    /** */
    private final BiPredicate<Row, Row> cond;

    /** */
    private final List<CorrelationId> correlationIds;

    /** */
    private final JoinRelType joinType;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final int leftInBufferSize;

    /** */
    private final int rightInBufferSize;

    /** */
    private final BitSet leftMatched = new BitSet();

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
    private Row rightEmptyRow;

    /** */
    private State state = State.INITIAL;

    /** */
    private enum State {
        /** */
        INITIAL,

        /** */
        FILLING_LEFT,

        /** */
        FILLING_RIGHT,

        /** */
        IDLE,

        /** */
        IN_LOOP,

        /** */
        END
    }

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public CorrelatedNestedLoopJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond,
        Set<CorrelationId> correlationIds, JoinRelType joinType) {
        super(ctx, rowType);

        assert !F.isEmpty(correlationIds);

        this.cond = cond;
        this.correlationIds = new ArrayList<>(correlationIds);
        this.joinType = joinType;

        leftInBufferSize = correlationIds.size();
        rightInBufferSize = IN_BUFFER_SIZE;

        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        onRequest();
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
                @Override public void push(Row row) throws Exception {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override public void end() throws Exception {
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
                @Override public void push(Row row) throws Exception {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override public void end() throws Exception {
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
    private void pushLeft(Row row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft--;

        if (leftInBuf == null)
            leftInBuf = new ArrayList<>(leftInBufferSize);

        leftInBuf.add(row);

        onPushLeft();
    }

    /** */
    private void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        if (rightInBuf == null)
            rightInBuf = new ArrayList<>(rightInBufferSize);

        rightInBuf.add(row);

        onPushRight();
    }

    /** */
    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft = -1;

        if (leftInBuf == null)
            leftInBuf = Collections.emptyList();

        onEndLeft();
    }

    /** */
    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight = -1;

        if (rightInBuf == null)
            rightInBuf = Collections.emptyList();

        onEndRight();
    }

    /** */
    private void onRequest() throws Exception {
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
                    checkState();

                    state = State.FILLING_LEFT;
                    leftSource().request(waitingLeft = leftInBufferSize);
                }, this::onError);

                break;
            case IDLE:
                assert rightInBuf != null;
                assert leftInBuf != null;
                assert waitingRight == -1 || waitingRight == 0 && rightInBuf.size() == rightInBufferSize;
                assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

                context().execute(() -> {
                    checkState();

                    join();
                }, this::onError);

                break;

            case END:
                downstream().end();
                break;

            default:
                throw new AssertionError("Unexpected state:" + state);
        }
    }

    /** */
    private void onPushLeft() throws Exception {
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
    private void onPushRight() throws Exception {
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
    private void onEndLeft() throws Exception {
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
    private void onEndRight() throws Exception {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert waitingRight == -1;
        assert !F.isEmpty(leftInBuf);
        assert waitingLeft == -1 || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        state = State.IDLE;

        join();
    }

    /** */
    private void join() throws Exception {
        assert state == State.IDLE;

        state = State.IN_LOOP;

        try {
            while (requested > 0 && rightIdx < rightInBuf.size()) {
                if (leftIdx == leftInBuf.size())
                    leftIdx = 0;

                while (requested > 0 && leftIdx < leftInBuf.size()) {
                    checkState();

                    Row left = leftInBuf.get(leftIdx);
                    Row right = rightInBuf.get(rightIdx);

                    if (cond.test(left, right)) {
                        leftMatched.set(leftIdx);

                        requested--;

                        Row row = handler.concat(left, right);

                        downstream().push(row);
                    }

                    leftIdx++;
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

            if (joinType == JoinRelType.LEFT && !F.isEmpty(leftInBuf)) {
                if (rightEmptyRow == null)
                    rightEmptyRow = handler.factory(context().getTypeFactory(), rightSource().rowType()).create();

                int notMatchedIdx = leftMatched.nextClearBit(0);

                while (requested > 0 && notMatchedIdx < leftInBuf.size()) {
                    downstream().push(handler.concat(leftInBuf.get(notMatchedIdx), rightEmptyRow));

                    requested--;

                    leftMatched.set(notMatchedIdx);

                    notMatchedIdx = leftMatched.nextClearBit(notMatchedIdx + 1);
                }

                if (requested == 0 && notMatchedIdx < leftInBuf.size())
                    return; // Some rows required to be pushed, wait for request.
            }

            if (waitingLeft == 0) {
                rightInBuf = null;
                leftInBuf = null;
                leftMatched.clear();

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

    /** */
    private void prepareCorrelations() {
        for (int i = 0; i < correlationIds.size(); i++) {
            Row row = i < leftInBuf.size() ? leftInBuf.get(i) : F.first(leftInBuf);
            context().setCorrelated(row, correlationIds.get(i).getId());
        }
    }
}
