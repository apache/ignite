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
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/** */
public abstract class MergeJoinNode<Row> extends AbstractNode<Row> {
    /** */
    private static final int HALF_BUF_SIZE = IN_BUFFER_SIZE >> 1;

    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    /** */
    protected final Comparator<Row> comp;

    /** */
    protected final RowHandler<Row> handler;

    /** */
    protected int requested;

    /** */
    protected int waitingLeft;

    /** */
    protected int waitingRight;

    /** */
    protected Row left;

    /** */
    protected Row right;

    /** */
    protected final Deque<Row> rightInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** Used to store similar rows of rights stream in many-to-many join mode. */
    protected List<Row> rightMaterialization;

    /** */
    protected boolean drainMaterialization;

    /** */
    protected int rightIdx;

    /** */
    protected boolean inLoop;

    /**
     * Flag indicating that at least one of the inputs has exchange underneath. In this case we can't prematurely end
     * downstream if one of the inputs is drained, we need to wait for both inputs, since async message from remote
     * node can reopen closed inbox, which can cause memory leaks.
     */
    protected final boolean distributed;

    /**
     * @param ctx Execution context.
     * @param comp Join expression.
     */
    private MergeJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp, boolean distributed) {
        super(ctx, rowType);

        this.comp = comp;
        this.distributed = distributed;
        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::doJoin, this::onError);
    }

    /** */
    private void doJoin() throws Exception {
        checkState();

        join();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        left = null;
        right = null;

        rightInBuf.clear();
        leftInBuf.clear();

        rightIdx = 0;
        rightMaterialization = null;
        drainMaterialization = false;
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
                    MergeJoinNode.this.onError(e);
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
                    MergeJoinNode.this.onError(e);
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

        leftInBuf.add(row);

        join();
    }

    /** */
    private void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        rightInBuf.add(row);

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
    protected abstract void join() throws Exception;

    /** Checks if finished considering strictly one shoulder. */
    protected void checkFinished(boolean leftShoulder) throws Exception {
        checkFinished(leftShoulder ? -1 : 1, false);
    }

    /**
     * Checks if finished. Can take in account one or both shoulders.
     *
     * @param shoulder If <0, checks the only left input. If is 0, checks both inputs. If >0, checks only right input.
     * @param strict Works with only with {@code input} == 0. If {@code true}, checks both inputs. Otherwise, checks any input.
     */
    protected void checkFinished(int shoulder, boolean strict) throws Exception {
        if (!distributed
            || (shoulder == 0 && !strict && (waitingLeft == NOT_WAITING || waitingRight == NOT_WAITING))
            || (shoulder == 0 && strict && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING)
            || (shoulder < 0 && waitingLeft == NOT_WAITING)
            || (shoulder > 0 && waitingRight == NOT_WAITING)
        ) {
            requested = 0;
            rightMaterialization = null;
            downstream().end();
        }
    }

    /** */
    @NotNull public static <Row> MergeJoinNode<Row> create(ExecutionContext<Row> ctx, RelDataType outputRowType, RelDataType leftRowType,
        RelDataType rightRowType, JoinRelType joinType, Comparator<Row> comp, boolean distributed) {
        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, outputRowType, comp, distributed);

            case LEFT: {
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new LeftJoin<>(ctx, outputRowType, comp, distributed, rightRowFactory);
            }

            case RIGHT: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);

                return new RightJoin<>(ctx, outputRowType, comp, distributed, leftRowFactory);
            }

            case FULL: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new FullOuterJoin<>(ctx, outputRowType, comp, distributed, leftRowFactory, rightRowFactory);
            }

            case SEMI:
                return new SemiJoin<>(ctx, outputRowType, comp, distributed);

            case ANTI:
                return new AntiJoin<>(ctx, outputRowType, comp, distributed);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    /** */
    private static class InnerJoin<Row> extends MergeJoinNode<Row> {
        /**
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param comp Join expression comparator.
         * @param distributed If one of the inputs has exchange underneath.
         */
        public InnerJoin(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp, boolean distributed) {
            super(ctx, rowType, comp, distributed);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) && (right != null || !rightInBuf.isEmpty()
                    || rightMaterialization != null)) {
                    checkState();

                    if (left == null)
                        left = leftInBuf.remove();

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING)
                            break;

                        if (!rightInBuf.isEmpty())
                            right = rightInBuf.remove();
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    Row row;
                    if (!drainMaterialization) {
                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null)
                                drainMaterialization = true;

                            continue;
                        }
                        else if (cmp > 0) {
                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty())
                                break;

                            if (comp.compare(left, rightInBuf.peek()) == 0)
                                rightMaterialization = new ArrayList<>();
                        }

                        row = handler.concat(left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        }
                        else
                            left = null;
                    }
                    else {
                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        Row right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        row = handler.concat(left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            }
            finally {
                inLoop = false;
            }

            tryToRequestInputs();

            if (requested > 0 && ((waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty())
                || (waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty() && rightMaterialization == null))
            )
                checkFinished(0, false);
        }
    }

    /** */
    private static class LeftJoin<Row> extends MergeJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched (hence pushed to downstream) or not. */
        private boolean matched;

        /**
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param comp Join expression comparator.
         * @param distributed If one of the inputs has exchange underneath.
         * @param rightRowFactory Right row factory.
         */
        public LeftJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            Comparator<Row> comp,
            boolean distributed,
            RowHandler.RowFactory<Row> rightRowFactory
        ) {
            super(ctx, rowType, comp, distributed);

            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) && (right != null || !rightInBuf.isEmpty()
                    || rightMaterialization != null || waitingRight == NOT_WAITING)) {
                    checkState();

                    if (left == null) {
                        left = leftInBuf.remove();

                        matched = false;
                    }

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING)
                            break;

                        if (!rightInBuf.isEmpty())
                            right = rightInBuf.remove();
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    Row row;
                    if (!drainMaterialization) {
                        if (right == null) {
                            row = handler.concat(left, rightRowFactory.create());

                            requested--;
                            downstream().push(row);

                            left = null;

                            continue;
                        }

                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            if (!matched) {
                                row = handler.concat(left, rightRowFactory.create());

                                requested--;
                                downstream().push(row);
                            }

                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null)
                                drainMaterialization = true;

                            continue;
                        }
                        else if (cmp > 0) {
                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        matched = true;

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty())
                                break;

                            if (comp.compare(left, rightInBuf.peek()) == 0)
                                rightMaterialization = new ArrayList<>();
                        }

                        row = handler.concat(left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        }
                        else
                            left = null;
                    }
                    else {
                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        Row right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        row = handler.concat(left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            }
            finally {
                inLoop = false;
            }

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty())
                checkFinished(true);
        }
    }

    /** */
    private static class RightJoin<Row> extends MergeJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Whether current right row was matched (hence pushed to downstream) or not. */
        private boolean matched;

        /**
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param comp Join expression comparator.
         * @param distributed If one of the inputs has exchange underneath.
         * @param leftRowFactory Left row factory.
         */
        public RightJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            Comparator<Row> comp,
            boolean distributed,
            RowHandler.RowFactory<Row> leftRowFactory
        ) {
            super(ctx, rowType, comp, distributed);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && !(left == null && leftInBuf.isEmpty() && waitingLeft != NOT_WAITING)
                    && (right != null || !rightInBuf.isEmpty() || rightMaterialization != null)) {
                    checkState();

                    if (left == null && !leftInBuf.isEmpty())
                        left = leftInBuf.remove();

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING)
                            break;

                        if (!rightInBuf.isEmpty()) {
                            right = rightInBuf.remove();

                            matched = false;
                        }
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    Row row;
                    if (!drainMaterialization) {
                        if (left == null) {
                            if (!matched) {
                                row = handler.concat(leftRowFactory.create(), right);

                                requested--;
                                downstream().push(row);
                            }

                            right = null;

                            continue;
                        }

                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null)
                                drainMaterialization = true;

                            continue;
                        }
                        else if (cmp > 0) {
                            if (!matched) {
                                row = handler.concat(leftRowFactory.create(), right);

                                requested--;
                                downstream().push(row);
                            }

                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty())
                                break;

                            if (comp.compare(left, rightInBuf.peek()) == 0)
                                rightMaterialization = new ArrayList<>();
                        }

                        matched = true;

                        row = handler.concat(left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        }
                        else
                            left = null;
                    }
                    else {
                        if (left == null) {
                            if (waitingLeft == NOT_WAITING) {
                                rightIdx = 0;
                                rightMaterialization = null;
                                drainMaterialization = false;
                            }

                            continue;
                        }

                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        Row right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        row = handler.concat(left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            }
            finally {
                inLoop = false;
            }

            tryToRequestInputs();

            if (requested > 0 && waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty() && rightMaterialization == null)
                checkFinished(false);
        }
    }

    /** */
    private static class FullOuterJoin<Row> extends MergeJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched (hence pushed to downstream) or not. */
        private boolean leftMatched;

        /** Whether current right row was matched (hence pushed to downstream) or not. */
        private boolean rightMatched;

        /**
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param comp Join expression comparator.
         * @param distributed If one of the inputs has exchange underneath.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        public FullOuterJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            Comparator<Row> comp,
            boolean distributed,
            RowHandler.RowFactory<Row> leftRowFactory,
            RowHandler.RowFactory<Row> rightRowFactory
        ) {
            super(ctx, rowType, comp, distributed);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && !(left == null && leftInBuf.isEmpty() && waitingLeft != NOT_WAITING)
                    && !(right == null && rightInBuf.isEmpty() && rightMaterialization == null && waitingRight != NOT_WAITING)) {
                    checkState();

                    if (left == null && !leftInBuf.isEmpty()) {
                        left = leftInBuf.remove();

                        leftMatched = false;
                    }

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING)
                            break;

                        if (!rightInBuf.isEmpty()) {
                            right = rightInBuf.remove();

                            rightMatched = false;
                        }
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    Row row;
                    if (!drainMaterialization) {
                        if (left == null || right == null) {
                            if (left == null && right != null) {
                                if (!rightMatched) {
                                    row = handler.concat(leftRowFactory.create(), right);

                                    requested--;
                                    downstream().push(row);
                                }

                                right = null;

                                continue;
                            }

                            if (left != null && right == null) {
                                if (!leftMatched) {
                                    row = handler.concat(left, rightRowFactory.create());

                                    requested--;
                                    downstream().push(row);
                                }

                                left = null;

                                continue;
                            }

                            break;
                        }

                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            if (!leftMatched) {
                                row = handler.concat(left, rightRowFactory.create());

                                requested--;
                                downstream().push(row);
                            }

                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null)
                                drainMaterialization = true;

                            continue;
                        }
                        else if (cmp > 0) {
                            if (!rightMatched) {
                                row = handler.concat(leftRowFactory.create(), right);

                                requested--;
                                downstream().push(row);
                            }

                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty())
                                break;

                            if (comp.compare(left, rightInBuf.peek()) == 0)
                                rightMaterialization = new ArrayList<>();
                        }

                        leftMatched = true;
                        rightMatched = true;

                        row = handler.concat(left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        }
                        else
                            left = null;
                    }
                    else {
                        if (left == null) {
                            if (waitingLeft == NOT_WAITING) {
                                rightIdx = 0;
                                rightMaterialization = null;
                                drainMaterialization = false;
                            }

                            continue;
                        }

                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        Row right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        leftMatched = true;

                        row = handler.concat(left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            }
            finally {
                inLoop = false;
            }

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty()
                && waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty() && rightMaterialization == null
            )
                checkFinished(0, true);
        }
    }

    /** */
    protected void tryToRequestInputs() throws Exception {
        if (waitingLeft == 0 && requested > 0 && leftInBuf.size() <= HALF_BUF_SIZE)
            leftSource().request(waitingLeft = IN_BUFFER_SIZE - leftInBuf.size());

        if (waitingRight == 0 && requested > 0 && rightInBuf.size() <= HALF_BUF_SIZE)
            rightSource().request(waitingRight = IN_BUFFER_SIZE - rightInBuf.size());
    }

    /** */
    private static class SemiJoin<Row> extends MergeJoinNode<Row> {
        /**
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param comp Join expression comparator.
         * @param distributed If one of the inputs has exchange underneath.
         */
        public SemiJoin(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp, boolean distributed) {
            super(ctx, rowType, comp, distributed);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) && (right != null || !rightInBuf.isEmpty())) {
                    checkState();

                    if (left == null)
                        left = leftInBuf.remove();

                    if (right == null)
                        right = rightInBuf.remove();

                    int cmp = comp.compare(left, right);

                    if (cmp < 0) {
                        left = null;

                        continue;
                    }
                    else if (cmp > 0) {
                        right = null;

                        continue;
                    }

                    requested--;
                    downstream().push(left);

                    left = null;
                }
            }
            finally {
                inLoop = false;
            }

            tryToRequestInputs();

            if (requested > 0 && ((waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty()
                || (waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty())))
            )
                checkFinished(0, false);
        }
    }

    /** */
    private static class AntiJoin<Row> extends MergeJoinNode<Row> {
        /**
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param comp Join expression comparator.
         * @param distributed If one of the inputs has exchange underneath.
         */
        public AntiJoin(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp, boolean distributed) {
            super(ctx, rowType, comp, distributed);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) &&
                    !(right == null && rightInBuf.isEmpty() && waitingRight != NOT_WAITING)) {
                    checkState();

                    if (left == null)
                        left = leftInBuf.remove();

                    if (right == null && !rightInBuf.isEmpty())
                        right = rightInBuf.remove();

                    if (right != null) {
                        int cmp = comp.compare(left, right);

                        if (cmp == 0) {
                            left = null;

                            continue;
                        }
                        else if (cmp > 0) {
                            right = null;

                            continue;
                        }
                    }

                    requested--;
                    downstream().push(left);

                    left = null;
                }
            }
            finally {
                inLoop = false;
            }

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty())
                checkFinished(true);
        }
    }
}
