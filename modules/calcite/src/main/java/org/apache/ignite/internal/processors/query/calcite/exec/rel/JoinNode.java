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
import org.apache.ignite.internal.util.typedef.F;

/**
 * TODO remove buffers.
 */
public class JoinNode extends AbstractNode<Object[]> {
    /** */
    private final Predicate<Object[]> condition;

    /** */
    private int requested;

    /** */
    private int waitingLeft;

    /** */
    private int waitingRight;

    /** */
    private List<Object[]> rightMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /** */
    private Deque<Object[]> leftInBuffer = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private boolean inLoop;

    /** */
    private Object[] left;

    /** */
    private int rightIdx;

    /**
     * @param ctx Execution context.
     * @param condition Join expression.
     */
    public JoinNode(ExecutionContext ctx, Predicate<Object[]> condition) {
        super(ctx);

        this.condition = condition;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 2;
        assert rowsCount > 0 && requested == 0;

        requested = rowsCount;

        if (!inLoop)
            context().execute(this::flushFromBuffer);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Object[]> requestDownstream(int idx) {
        if (idx == 0)
            return new Downstream<Object[]>() {
                /** {@inheritDoc} */
                @Override public void push(Object[] row) {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override public void end() {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override public void onError(Throwable e) {
                    JoinNode.this.onError(e);
                }
            };
        else if (idx == 1)
            return new Downstream<Object[]>() {
                /** {@inheritDoc} */
                @Override public void push(Object[] row) {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override public void end() {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override public void onError(Throwable e) {
                    JoinNode.this.onError(e);
                }
            };

        throw new IndexOutOfBoundsException();
    }

    /** */
    private void pushLeft(Object[] row) {
        checkThread();

        assert downstream != null;
        assert waitingLeft > 0;

        leftInBuffer.add(row);

        flushFromBuffer();
    }

    /** */
    private void pushRight(Object[] row) {
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
                while (requested > 0 && (left != null || !leftInBuffer.isEmpty())) {
                    if (left == null)
                        left = leftInBuffer.remove();

                    while (requested > 0 && rightIdx < rightMaterialized.size()) {
                        Object[] row = F.concat(left, rightMaterialized.get(rightIdx++));

                        if (!condition.test(row))
                            continue;

                        requested--;
                        downstream.push(row);
                    }

                    if (rightIdx == rightMaterialized.size()) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuffer.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == -1 && waitingRight == -1 && left == null && leftInBuffer.isEmpty()) {
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
