/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiPredicate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MappingRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoinInfo;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** Hash join implementor. */
public abstract class HashJoinNode<Row> extends AbstractRightMaterializedJoinNode<Row> {
    /** */
    private static final int INITIAL_CAPACITY = 128;

    /** */
    private final RowHandler<Row> leftRowHnd;

    /** */
    private final RowHandler<Row> rightRowHnd;

    /** */
    private final boolean keepRowsWithNull;

    /** Output row handler. */
    protected final RowHandler<Row> outRowHnd;

    /** Right rows storage. */
    protected Map<GroupKey<Row>, TouchedArrayList<Row>> hashStore = new HashMap<>(INITIAL_CAPACITY);

    /** */
    protected Iterator<Row> rightIt = Collections.emptyIterator();

    /** */
    @Nullable protected final BiPredicate<Row, Row> nonEqCond;

    /**
     * Creates hash join node.
     *
     * @param ctx Execution context.
     * @param rowType Out row type.
     * @param info Join info.
     * @param outRowHnd Output row handler.
     * @param keepRowsWithNull {@code True} if we need to store the row from right shoulder even if it contains NULL in
     *                         any of join key position. This is required for joins which emit unmatched part
     *                         of the right shoulder, such as RIGHT JOIN and FULL OUTER JOIN.
     * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
     */
    protected HashJoinNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        IgniteJoinInfo info,
        RowHandler<Row> outRowHnd,
        boolean keepRowsWithNull,
        @Nullable BiPredicate<Row, Row> nonEqCond
    ) {
        super(ctx, rowType);

        this.keepRowsWithNull = keepRowsWithNull;

        leftRowHnd = new MappingRowHandler<>(ctx.rowHandler(), info.leftKeys.toIntArray());
        rightRowHnd = new MappingRowHandler<>(ctx.rowHandler(), info.rightKeys.toIntArray());

        this.outRowHnd = outRowHnd;

        this.nonEqCond = nonEqCond;
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        super.rewindInternal();

        rightIt = Collections.emptyIterator();

        hashStore.clear();
    }

    /** Creates certain join node. */
    public static <RowT> HashJoinNode<RowT> create(
        ExecutionContext<RowT> ctx,
        RelDataType rowType,
        RelDataType leftRowType,
        RelDataType rightRowType,
        JoinRelType type,
        IgniteJoinInfo info,
        @Nullable BiPredicate<RowT, RowT> nonEqCond
    ) {
        assert !info.pairs().isEmpty() && (info.isEqui() || type == JoinRelType.INNER || type == JoinRelType.SEMI);
        assert nonEqCond == null || type == JoinRelType.INNER || type == JoinRelType.SEMI || type == JoinRelType.LEFT;

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        RowHandler<RowT> rowHnd = ctx.rowHandler();

        switch (type) {
            case INNER:
                return new InnerHashJoin<>(ctx, rowType, info, rowHnd, nonEqCond);

            case LEFT:
                return new LeftHashJoin<>(ctx, rowType, info, rowHnd, rowHnd.factory(typeFactory, rightRowType), nonEqCond);

            case RIGHT:
                return new RightHashJoin<>(ctx, rowType, info, rowHnd, rowHnd.factory(typeFactory, leftRowType));

            case FULL: {
                return new FullOuterHashJoin<>(ctx, rowType, info, rowHnd, rowHnd.factory(typeFactory, leftRowType),
                    rowHnd.factory(typeFactory, rightRowType), nonEqCond);
            }

            case SEMI:
                return new SemiHashJoin<>(ctx, rowType, info, rowHnd, nonEqCond);

            case ANTI:
                return new AntiHashJoin<>(ctx, rowType, info, rowHnd, nonEqCond);

            default:
                throw new IllegalArgumentException("Join of type '" + type + "' isn't supported.");
        }
    }

    /** */
    protected Collection<Row> lookup(Row row) {
        GroupKey<Row> key = GroupKey.of(row, leftRowHnd, false);

        if (key == null)
            return Collections.emptyList();

        TouchedArrayList<Row> res = hashStore.get(key);

        if (res == null)
            return Collections.emptyList();

        res.touched = true;

        return res;
    }

    /** */
    protected Iterator<Row> untouched() {
        return F.flat(F.iterator(hashStore.values(), c0 -> c0, true, c1 -> !c1.touched));
    }

    /** {@inheritDoc} */
    @Override protected void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        nodeMemoryTracker.onRowAdded(row);

        waitingRight--;

        GroupKey<Row> key = GroupKey.of(row, rightRowHnd, keepRowsWithNull);

        if (key != null)
            hashStore.computeIfAbsent(key, k -> new TouchedArrayList<>()).add(row);

        if (waitingRight == 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }

    /** */
    protected boolean leftFinished() {
        return waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty();
    }

    /** */
    protected boolean rightFinished() {
        return waitingRight == NOT_WAITING && !rightIt.hasNext();
    }

    /** */
    protected boolean checkJoinFinished() throws Exception {
        if (requested > 0 && leftFinished() && rightFinished()) {
            requested = 0;

            hashStore.clear();

            downstream().end();

            return true;
        }

        return false;
    }

    /** */
    private static final class InnerHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates node for INNER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private InnerHashJoin(ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, false, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        // Proceed with next left row, if previous was fully processed.
                        if (left == null) {
                            left = leftInBuf.remove();

                            rightIt = lookup(left).iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (requested > 0 && rightIt.hasNext()) {
                                if (rescheduleJoin())
                                    return;

                                RowT right = rightIt.next();

                                if (nonEqCond != null && !nonEqCond.test(left, right))
                                    continue;

                                --requested;

                                downstream().push(outRowHnd.concat(left, right));
                            }

                            if (!rightIt.hasNext())
                                left = null;
                        }
                        else
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static final class LeftHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /**
         * Creates node for LEFT OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param info Join info.
         * @param rowType Out row type.
         * @param outRowHnd Output row handler.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond Non-equi conditions.
         */
        private LeftHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, false, nonEqCond);

            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        // Proceed with next left row, if previous was fully processed.
                        if (left == null) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            if (rightRows.isEmpty()) {
                                requested--;

                                downstream().push(outRowHnd.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emit unmatched left row.
                            while (requested > 0 && rightIt.hasNext()) {
                                if (rescheduleJoin())
                                    return;

                                RowT right = rightIt.next();

                                if (nonEqCond != null && !nonEqCond.test(left, right))
                                    continue;

                                 --requested;

                                downstream().push(outRowHnd.concat(left, right));
                            }

                            if (!rightIt.hasNext())
                                left = null;
                        }
                        else
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static final class RightHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** */
        private boolean drainMaterialization;

        /**
         * Creates node for RIGHT OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param leftRowFactory Left row factory.
         */
        private RightHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, rowType, info, outRowHnd, true, null);

            assert nonEqCond == null : "Non equi condition is not supported in RIGHT join";

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        // Proceed with next left row, if previous was fully processed.
                        if (left == null) {
                            left = leftInBuf.remove();

                            rightIt = lookup(left).iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (requested > 0 && rightIt.hasNext()) {
                                if (rescheduleJoin())
                                    return;

                                RowT right = rightIt.next();

                                --requested;

                                downstream().push(outRowHnd.concat(left, right));
                            }

                            if (!rightIt.hasNext())
                                left = null;
                        }
                        else
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            // Emit unmatched right rows.
            if (leftFinished() && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;

                try {
                    if (!rightIt.hasNext() && !drainMaterialization) {
                        // Prevent scanning store more than once.
                        drainMaterialization = true;

                        rightIt = untouched();
                    }

                    RowT emptyLeft = leftRowFactory.create();

                    while (requested > 0 && rightIt.hasNext()) {
                        if (rescheduleJoin())
                            return;

                        RowT right = rightIt.next();

                        RowT row = outRowHnd.concat(emptyLeft, right);

                        --requested;

                        downstream().push(row);
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            drainMaterialization = false;

            super.rewindInternal();
        }
    }

    /** */
    private static final class FullOuterHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /** */
        private boolean drainMaterialization;

        /**
         * Creates node for FULL OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        private FullOuterHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory,
            RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, true, null);

            assert nonEqCond == null : "Non equi condition is not supported in FULL OUTER join";

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        // Proceed with next left row, if previous was fully processed.
                        if (left == null) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            if (rightRows.isEmpty()) {
                                // Emit empty right row for unmatched left row.
                                rightIt = Collections.singletonList(rightRowFactory.create()).iterator();
                            }
                            else
                                rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (requested > 0 && rightIt.hasNext()) {
                                if (rescheduleJoin())
                                    return;

                                RowT right = rightIt.next();

                                --requested;

                                downstream().push(outRowHnd.concat(left, right));
                            }

                            if (!rightIt.hasNext())
                                left = null;
                        }
                        else
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            // Emit unmatched right rows.
            if (leftFinished() && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;

                try {
                    if (!rightIt.hasNext() && !drainMaterialization) {
                        // Prevent scanning store more than once.
                        drainMaterialization = true;

                        rightIt = untouched();
                    }

                    RowT emptyLeft = leftRowFactory.create();

                    while (requested > 0 && rightIt.hasNext()) {
                        if (rescheduleJoin())
                            return;

                        RowT right = rightIt.next();

                        RowT row = outRowHnd.concat(emptyLeft, right);

                        --requested;

                        downstream().push(row);
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            drainMaterialization = false;

            super.rewindInternal();
        }
    }

    /** */
    private static final class SemiHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates node for SEMI JOIN operator.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private SemiHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, false, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        // Proceed with next left row, if previous was fully processed.
                        if (left == null) {
                            left = leftInBuf.remove();

                            rightIt = lookup(left).iterator();
                        }

                        boolean anyMatched = rightIt.hasNext() && nonEqCond == null;

                        if (!anyMatched) {
                            // Find any matched row.
                            while (rightIt.hasNext()) {
                                RowT right = rightIt.next();

                                if (nonEqCond.test(left, right)) {
                                    anyMatched = true;

                                    break;
                                }

                                if (rescheduleJoin())
                                    return;
                            }
                        }

                        if (anyMatched) {
                            requested--;

                            downstream().push(left);

                            rightIt = Collections.emptyIterator();
                        }

                        if (!rightIt.hasNext())
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static final class AntiHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates node for ANTI JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param nonEqCond Non-equi conditions.
         */
        private AntiHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, false, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (rescheduleJoin())
                            return;

                        left = leftInBuf.remove();

                        Collection<RowT> rightRows = lookup(left);

                        if (rightRows.isEmpty()) {
                            requested--;

                            downstream().push(left);
                        }

                        left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static final class TouchedArrayList<T> extends ArrayList<T> {
        /** */
        private boolean touched;
    }
}
