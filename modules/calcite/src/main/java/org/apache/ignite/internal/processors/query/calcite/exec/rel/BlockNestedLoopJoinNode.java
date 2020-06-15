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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class BlockNestedLoopJoinNode<Row> extends AbstractNode<Row> {
    /** */
    private static final List<?> EMPTY = ImmutableList.of();

    /** */
    private final Predicate<Row> cond;

    /** */
    private final RowHandler<Row> handler;

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
    int leftIdx;

    /** */
    int rightIdx;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public BlockNestedLoopJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond) {
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
            context().execute(this::doJoin);
    }

    /** */
    private void doJoin() {
        try {
            doJoinInternal();
        }
        catch (Exception e) {
            onError(e);
        }
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
                    BlockNestedLoopJoinNode.this.onError(e);
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
                    BlockNestedLoopJoinNode.this.onError(e);
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
                leftInBuf = new ArrayList<>(IN_BUFFER_SIZE);

            leftInBuf.add(row);

            doJoinInternal();
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
                rightInBuf = new ArrayList<>(IN_BUFFER_SIZE);

            rightInBuf.add(row);

            doJoinInternal();
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
                leftInBuf = (List<Row>)EMPTY;

            waitingLeft = -1;

            doJoinInternal();
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
                rightInBuf = (List<Row>)EMPTY;

            waitingRight = -1;

            doJoinInternal();
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
    private void doJoinInternal() {
        // 1. fill left buffer
        if (waitingLeft == 0 && F.isEmpty(leftInBuf))
            sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

        // 2. fill right buffer
        if (waitingRight == 0 && F.isEmpty(rightInBuf))
            sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

        // do not fill right buffer untill there is a row in the left buffer.
        if (!F.isEmpty(leftInBuf) && waitingRight == -1 && rightInBuf == null) {
            sources.get(1).reset();
            sources.get(1).request(waitingRight = IN_BUFFER_SIZE);
        }

        if (waitingLeft != -1 && (leftInBuf == null || leftInBuf.size() != IN_BUFFER_SIZE))
            return;

        if (waitingRight != -1 && (rightInBuf == null || rightInBuf.size() != IN_BUFFER_SIZE))
            return;

        assert waitingRight <= 0 && waitingLeft <= 0;
        assert waitingRight == -1 || rightInBuf.size() == IN_BUFFER_SIZE;
        assert waitingLeft == -1 || leftInBuf.size() == IN_BUFFER_SIZE;

        if (leftInBuf != null && rightInBuf != null) {
            inLoop = true;
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
                    }

                    if (leftIdx == leftInBuf.size())
                        rightInBuf.set(rightIdx++, null);
                }
            }
            finally {
                inLoop = false;
            }

            if (rightIdx == rightInBuf.size()) {
                rightInBuf = null;
                rightIdx = 0;

                if (waitingRight == -1) {
                    leftInBuf = null;
                    leftIdx = 0;
                }
            }
        }

        if (waitingLeft == 0 && F.isEmpty(leftInBuf))
            sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

        if (waitingRight == 0 && F.isEmpty(rightInBuf))
            sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

        if (waitingRight == -1 && waitingLeft == -1 && F.isEmpty(rightInBuf) && F.isEmpty(leftInBuf) && requested > 0) {
            requested = 0;
            downstream.end();
        }
    }
}
