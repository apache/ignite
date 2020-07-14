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
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/** */
public abstract class NestedLoopJoinNode<Row> extends AbstractNode<Row> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    /** */
    protected final Predicate<Row> cond;

    /** */
    protected final RowHandler<Row> handler;

    /** */
    protected int requested;

    /** */
    protected int waitingLeft;

    /** */
    protected int waitingRight;

    /** */
    protected final List<Row> rightMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /** */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    protected boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    private NestedLoopJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond) {
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
                    NestedLoopJoinNode.this.onError(e);
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
                    NestedLoopJoinNode.this.onError(e);
                }
            };

        throw new IndexOutOfBoundsException();
    }

    /** {@inheritDoc} */
    @Override protected void resetInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        rightMaterialized.clear();
        leftInBuf.clear();
    }

    /** */
    private void pushLeft(Row row) {
        checkThread();

        assert downstream != null;
        assert waitingLeft > 0;

        waitingLeft--;

        leftInBuf.add(row);

        try {
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
            rightMaterialized.add(row);

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);
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

        waitingLeft = NOT_WAITING;

        try {
            doJoinInternal();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** */
    private void endRight() {
        checkThread();

        assert downstream != null;
        assert waitingRight > 0;

        waitingRight = NOT_WAITING;

        try {
            doJoinInternal();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** */
    private void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** */
    protected abstract void doJoinInternal();

    /** */
    @NotNull public static <Row> NestedLoopJoinNode<Row> create(ExecutionContext<Row> ctx, RelDataType leftRowType,
        RelDataType rightRowType, JoinRelType joinType, Predicate<Row> cond) {
        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, cond);

            case LEFT: {
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new LeftJoin<>(ctx, cond, rightRowFactory);
            }

            case RIGHT: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);

                return new RightJoin<>(ctx, cond, leftRowFactory);
            }

            case FULL: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new FullOuterJoin<>(ctx, cond, leftRowFactory, rightRowFactory);
            }

            case SEMI:
                return new SemiJoin<>(ctx, cond);

            case ANTI:
                return new AntiJoin<>(ctx, cond);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    /** */
    private static class InnerJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private Row left;

        /** */
        private int rightIdx;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public InnerJoin(ExecutionContext<Row> ctx, Predicate<Row> cond) {
            super(ctx, cond);
        }

        /** {@inheritDoc} */
        @Override protected void resetInternal() {
            left = null;
            rightIdx = 0;

            super.resetInternal();
        }

        /** */
        @Override protected void doJoinInternal() {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null)
                            left = leftInBuf.remove();

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            Row row = handler.concat(left, rightMaterialized.get(rightIdx++));

                            if (!cond.test(row))
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
                finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream.end();
            }
        }
    }

    /** */
    private static class LeftJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean matched;

        /** */
        private Row left;

        /** */
        private int rightIdx;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public LeftJoin(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> rightRowFactory) {
            super(ctx, cond);

            this.rightRowFactory = rightRowFactory;
        }

        /** */
        @Override protected void resetInternal() {
            matched = false;
            left = null;
            rightIdx = 0;

            super.resetInternal();
        }

        /** {@inheritDoc} */
        @Override protected void doJoinInternal() {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
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
                finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream.end();
            }
        }
    }

    /** */
    private static class RightJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** */
        private BitSet rightNotMatchedIndexes;

        /** */
        private int lastPushedInd;

        /** */
        private Row left;

        /** */
        private int rightIdx;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public RightJoin(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> leftRowFactory) {
            super(ctx, cond);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void resetInternal() {
            left = null;
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;
            rightIdx = 0;

            super.resetInternal();
        }

        /** {@inheritDoc} */
        @Override protected void doJoinInternal() {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null)
                            left = leftInBuf.remove();

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            Row right = rightMaterialized.get(rightIdx++);
                            Row joined = handler.concat(left, right);

                            if (!cond.test(joined))
                                continue;

                            requested--;
                            rightNotMatchedIndexes.clear(rightIdx - 1);
                            downstream.push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && !rightNotMatchedIndexes.isEmpty()) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd);;
                        lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        if (lastPushedInd < 0)
                            break;

                        Row row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream.push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0)
                            break;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream.end();
            }
        }
    }

    /** */
    private static class FullOuterJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean leftMatched;

        /** */
        private BitSet rightNotMatchedIndexes;

        /** */
        private int lastPushedInd;

        /** */
        private Row left;

        /** */
        private int rightIdx;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public FullOuterJoin(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> leftRowFactory,
            RowHandler.RowFactory<Row> rightRowFactory) {
            super(ctx, cond);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void resetInternal() {
            left = null;
            leftMatched = false;
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;
            rightIdx = 0;

            super.resetInternal();
        }

        /** {@inheritDoc} */
        @Override protected void doJoinInternal() {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            leftMatched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            Row right = rightMaterialized.get(rightIdx++);
                            Row joined = handler.concat(left, right);

                            if (!cond.test(joined))
                                continue;

                            requested--;
                            leftMatched = true;
                            rightNotMatchedIndexes.clear(rightIdx - 1);
                            downstream.push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!leftMatched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream.push(handler.concat(left, rightRowFactory.create()));
                            }

                            if (leftMatched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && !rightNotMatchedIndexes.isEmpty()) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd);;
                        lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        if (lastPushedInd < 0)
                            break;

                        Row row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream.push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0)
                            break;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream.end();
            }
        }
    }

    /** */
    private static class SemiJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private Row left;

        /** */
        private int rightIdx;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public SemiJoin(ExecutionContext<Row> ctx, Predicate<Row> cond) {
            super(ctx, cond);
        }

        /** {@inheritDoc} */
        @Override protected void resetInternal() {
            left = null;
            rightIdx = 0;

            super.resetInternal();
        }

        /** {@inheritDoc} */
        @Override protected void doJoinInternal() {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null)
                        left = leftInBuf.remove();

                    boolean matched = false;

                    while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                        Row row = handler.concat(left, rightMaterialized.get(rightIdx++));

                        if (!cond.test(row))
                            continue;

                        requested--;
                        matched = true;
                        downstream.push(left);
                    }

                    if (matched || rightIdx == rightMaterialized.size()) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                && leftInBuf.isEmpty()) {
                downstream.end();
                requested = 0;
            }
        }
    }

    /** */
    private static class AntiJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private Row left;

        /** */
        private int rightIdx;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public AntiJoin(ExecutionContext<Row> ctx, Predicate<Row> cond) {
            super(ctx, cond);
        }

        /** */
        @Override protected void resetInternal() {
            left = null;
            rightIdx = 0;

            super.resetInternal();
        }

        /** {@inheritDoc} */
        @Override protected void doJoinInternal() {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null)
                            left = leftInBuf.remove();

                        boolean matched = false;

                        while (!matched && rightIdx < rightMaterialized.size()) {
                            Row row = handler.concat(left, rightMaterialized.get(rightIdx++));

                            if (cond.test(row))
                                matched = true;
                        }

                        if (!matched) {
                            requested--;
                            downstream.push(left);
                        }

                        left = null;
                        rightIdx = 0;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream.end();
            }
        }
    }
}
