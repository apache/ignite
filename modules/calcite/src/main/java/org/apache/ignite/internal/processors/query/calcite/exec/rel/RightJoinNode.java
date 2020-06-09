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
public class RightJoinNode<Row> extends AbstractNode<Row> {
    /** Right row factory. */
    private final RowHandler.RowFactory<Row> leftRowFactory;

    /** Whether current right row was matched or not. */
    private boolean matched;

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
    private final List<Row> leftMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /** */
    private final Deque<Row> rightInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private boolean inLoop;

    /** */
    private Row right;

    /** */
    private int leftIdx;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public RightJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> leftRowFactory) {
        super(ctx);

        this.cond = cond;
        this.leftRowFactory = leftRowFactory;
        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 2;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::flushFromBuffer);
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
                    RightJoinNode.this.onError(e);
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
                    RightJoinNode.this.onError(e);
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

        leftMaterialized.add(row);

        if (waitingLeft == 0)
            sources.get(1).request(waitingLeft = IN_BUFFER_SIZE);

    }

    /** */
    private void pushRight(Row row) {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        waitingRight--;

        rightInBuf.add(row);

        flushFromBuffer();
    }

    /** */
    private void endLeft() {
        checkThread();

        assert downstream != null;
        assert waitingLeft > 0;

        waitingLeft = -1;

        flushFromBuffer();
    }

    /** */
    private void endRight() {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        waitingRight = -1;

        flushFromBuffer();
    }

    /** */
    private void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** */
    private void flushFromBuffer() {
        inLoop = true;
        try {
            if (waitingLeft == -1) {
                while (requested > 0 && (right != null || !rightInBuf.isEmpty())) {
                    if (right == null) {
                        right = rightInBuf.remove();
                        
                        matched = false;
                    }

                    while (requested > 0 && leftIdx < leftMaterialized.size()) {
                        Row row = handler.concat(leftMaterialized.get(leftIdx++), right);

                        if (!cond.test(row))
                            continue;

                        requested--;
                        matched = true;
                        downstream.push(row);
                    }

                    if (leftIdx == leftMaterialized.size()) {
                        boolean wasPushed = false;

                        if (!matched && requested > 0) {
                            requested--;
                            wasPushed = true;

                            downstream.push(handler.concat(leftRowFactory.create(), right));
                        }

                        if (matched || wasPushed) {
                            right = null;
                            leftIdx = 0;
                        }
                    }
                }
            }

            if (waitingLeft == 0 && rightInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == -1 && waitingRight == -1 && right == null && rightInBuf.isEmpty()) {
                downstream.end();
                requested = 0;
            }
        }
        catch (Exception e) {
            downstream.onError(e);
        }
        finally {
            inLoop = false;
        }
    }
}
