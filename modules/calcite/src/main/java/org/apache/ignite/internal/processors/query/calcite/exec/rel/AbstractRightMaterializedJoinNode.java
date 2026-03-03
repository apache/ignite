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
import java.util.Deque;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** Right-part materialized join node. Holds data from the right part locally. */
public abstract class AbstractRightMaterializedJoinNode<Row> extends MemoryTrackingNode<Row> {
    /** */
    protected static final int HALF_BUF_SIZE = IN_BUFFER_SIZE >> 1;

    /** Special flag which marks that all the rows are received. */
    protected static final int NOT_WAITING = -1;

    /** */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    protected boolean inLoop;

    /** */
    protected int requested;

    /** */
    protected int waitingLeft;

    /** */
    protected int waitingRight;

    /** */
    protected @Nullable Row left;

    /** */
    protected int processed;

    /** */
    protected AbstractRightMaterializedJoinNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        super(ctx, rowType);
    }

    /** */
    protected abstract void join() throws Exception;

    /** */
    protected abstract void pushRight(Row row) throws Exception;

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::join0, this::onError);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;
        left = null;
        processed = 0;

        leftInBuf.clear();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx == 0) {
            return new Downstream<>() {
                @Override public void push(Row row) throws Exception {
                    pushLeft(row);
                }

                @Override public void end() throws Exception {
                    endLeft();
                }

                @Override public void onError(Throwable e) {
                    AbstractRightMaterializedJoinNode.this.onError(e);
                }
            };
        }
        else if (idx == 1) {
            return new Downstream<>() {
                @Override public void push(Row row) throws Exception {
                    pushRight(row);
                }

                @Override public void end() throws Exception {
                    endRight();
                }

                @Override public void onError(Throwable e) {
                    AbstractRightMaterializedJoinNode.this.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    /** */
    private void pushLeft(Row row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        --waitingLeft;

        leftInBuf.add(row);

        join0();
    }

    /** */
    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft = NOT_WAITING;

        join0();
    }

    /** */
    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight = NOT_WAITING;

        join0();
    }

    /** */
    protected void tryToRequestInputs() throws Exception {
        if (waitingLeft == 0 && leftInBuf.size() <= HALF_BUF_SIZE)
            leftSource().request(waitingLeft = IN_BUFFER_SIZE - leftInBuf.size());

        if (waitingRight == 0 && requested > 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }

    /** */
    protected Node<Row> leftSource() {
        return sources().get(0);
    }

    /** */
    protected Node<Row> rightSource() {
        return sources().get(1);
    }

    /** */
    private void join0() throws Exception {
        checkState();

        processed = 0;

        join();
    }

    /** */
    protected boolean rescheduleJoin() {
        if (processed++ > IN_BUFFER_SIZE) {
            context().execute(this::join0, this::onError);

            return true;
        }

        return false;
    }
}
