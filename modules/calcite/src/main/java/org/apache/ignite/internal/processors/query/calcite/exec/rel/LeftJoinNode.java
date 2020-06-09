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
public class LeftJoinNode<Row> extends AbstractNode<Row> {
    /** Right row factory. */
    private final RowHandler.RowFactory<Row> rightRowFactory;

    /** Whether current left row was matched or not. */
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
    private final List<Row> rightMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /** */
    private final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private boolean inLoop;

    /** */
    private Row left;

    /** */
    private int rightIdx;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public LeftJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> rightRowFactory) {
        super(ctx);

        this.cond = cond;
        this.rightRowFactory = rightRowFactory;
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
                    LeftJoinNode.this.onError(e);
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
                    LeftJoinNode.this.onError(e);
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

        flushFromBuffer();
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
            if (waitingRight == -1) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null) {
                        left = leftInBuf.remove();

                        matched = false;
                    }

                    while (requested > 0 && rightIdx < rightMaterialized.size()) {
                        Row row = handler.concat(left, rightMaterialized.get(rightIdx++));

                        if (!cond.test(row))
                            continue;

                        requested--;
                        matched = true;
                        downstream.push(row);
                    }

                    if (rightIdx == rightMaterialized.size()) {
                        boolean wasPushed = false;

                        if (!matched && requested > 0) {
                            requested--;
                            wasPushed = true;

                            downstream.push(handler.concat(left, rightRowFactory.create()));
                        }

                        if (matched || wasPushed) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == -1 && waitingRight == -1 && left == null && leftInBuf.isEmpty()) {
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
