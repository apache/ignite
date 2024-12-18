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
public abstract class AbstractRightMaterializedJoinNode<Row> extends AbstractNode<Row> {
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

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::doJoin, this::onError);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;
        left = null;

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

        checkState();

        --waitingLeft;

        leftInBuf.add(row);

        join();
    }

    /** */
    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft = NOT_WAITING;

        join();
    }

    /** */
    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight = NOT_WAITING;

        join();
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
    private void doJoin() throws Exception {
        checkState();

        join();
    }
}
