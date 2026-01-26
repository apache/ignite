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
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MappingRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoinInfo;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/** Hash join implementor. */
public abstract class HashJoinNode<Row> extends AbstractRightMaterializedJoinNode<Row> {
    /**
     * Creates hash join node.
     *
     * @param ctx Execution context.
     * @param rowType Out row type.
     */
    protected HashJoinNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType
    ) {
        super(ctx, rowType);
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
        assert !info.pairs().isEmpty();

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        RowHandler<RowT> rowHnd = ctx.rowHandler();

        switch (type) {
            case INNER:
                return new InnerHashJoin<>(ctx, rowType, info, rowHnd, nonEqCond);

            case LEFT:
                return new LeftHashJoin<>(ctx, rowType, info, rowHnd, rowHnd.factory(typeFactory, rightRowType),
                    nonEqCond);

            case RIGHT:
                if (nonEqCond == null) {
                    return new RightHashJoin<>(ctx, rowType, info, rowHnd,
                        rowHnd.factory(typeFactory, leftRowType), nonEqCond);
                }
                else {
                    return new RightRowTouchingHashJoin<>(ctx, rowType, info, rowHnd,
                        rowHnd.factory(typeFactory, leftRowType), nonEqCond);
                }

            case FULL:
                if (nonEqCond == null) {
                    return new FullOuterHashJoin<>(ctx, rowType, info, rowHnd,
                        rowHnd.factory(typeFactory, leftRowType), rowHnd.factory(typeFactory, rightRowType), nonEqCond);
                }
                else {
                    return new FullOuterRowTouchingHashJoin<>(ctx, rowType, info, rowHnd,
                        rowHnd.factory(typeFactory, leftRowType), rowHnd.factory(typeFactory, rightRowType), nonEqCond);
                }

            case SEMI:
                return new SemiHashJoin<>(ctx, rowType, info, nonEqCond);

            case ANTI:
                return new AntiHashJoin<>(ctx, rowType, info, nonEqCond);

            default:
                throw new IllegalArgumentException("Join of type '" + type + "' isn't supported.");
        }
    }

    /** */
    private abstract static class AbstractStoringHashJoin<Row, RowList extends List<Row>> extends HashJoinNode<Row> {
        /** */
        private static final int INITIAL_CAPACITY = 128;

        /** */
        private final RowHandler<Row> leftRowHnd;

        /** */
        private final RowHandler<Row> rightRowHnd;

        /** */
        private final boolean keepRowsWithNull;

        /** */
        private final ImmutableBitSet allowNulls;

        /** */
        protected @Nullable RowList rightRows;

        /** */
        protected int rightIdx;

        /** */
        @Nullable protected final BiPredicate<Row, Row> nonEqCond;

        /** Right rows storage. */
        protected Map<GroupKey<Row>, RowList> hashStore = new HashMap<>(INITIAL_CAPACITY);

        /**
         * Constructor.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param keepRowsWithNull {@code True} if we need to store the row from right hand even if it contains NULL in
         *                         any of join key position. This is required for joins which emit unmatched part
         *                         of the right hand, such as RIGHT JOIN and FULL OUTER JOIN.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        protected AbstractStoringHashJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            boolean keepRowsWithNull,
            @Nullable BiPredicate<Row, Row> nonEqCond
        ) {
            super(ctx, rowType);

            allowNulls = info.allowNulls();
            this.keepRowsWithNull = keepRowsWithNull;

            leftRowHnd = new MappingRowHandler<>(ctx.rowHandler(), info.leftKeys.toIntArray());
            rightRowHnd = new MappingRowHandler<>(ctx.rowHandler(), info.rightKeys.toIntArray());

            this.nonEqCond = nonEqCond;
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            rightRows = null;
            rightIdx = 0;

            hashStore.clear();
        }

        /** */
        protected @Nullable RowList lookup(Row row) {
            GroupKey<Row> key = GroupKey.of(row, leftRowHnd, allowNulls);

            if (key == null)
                return null;

            return hashStore.get(key);
        }

        /** {@inheritDoc} */
        @Override protected void pushRight(Row row) throws Exception {
            assert downstream() != null;
            assert waitingRight > 0;

            waitingRight--;

            GroupKey<Row> key = keepRowsWithNull ? GroupKey.of(row, rightRowHnd) : GroupKey.of(row, rightRowHnd, allowNulls);

            if (key != null) {
                nodeMemoryTracker.onRowAdded(row);

                hashStore.computeIfAbsent(key, k -> createRowList()).add(row);
            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);
        }

        /** */
        protected abstract RowList createRowList();

        /** */
        protected boolean hasNextRight() {
            return rightRows != null && rightIdx < rightRows.size();
        }

        /** */
        protected Row nextRight() {
            return rightRows.get(rightIdx++);
        }

        /** */
        protected boolean leftFinished() {
            return waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty();
        }

        /** */
        protected boolean rightFinished() {
            return waitingRight == NOT_WAITING && !hasNextRight();
        }

        /** */
        protected boolean checkNextLeftRowStarted() {
            // Proceed with next left row, if previous was fully processed.
            if (left == null) {
                left = leftInBuf.remove();

                rightRows = lookup(left);
                rightIdx = 0;

                return true;
            }

            return false;
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
    }

    /** */
    private abstract static class AbstractMatchingHashJoin<Row, RowList extends List<Row>>
        extends AbstractStoringHashJoin<Row, RowList> {
        /** Output row factory. */
        private final BiFunction<Row, Row, Row> outRowFactory;

        /** Empty right row. */
        private final @Nullable Row emptyRightRow;

        /** Empty left row. */
        private final @Nullable Row emptyLeftRow;

        /** Whether current left row was matched. */
        protected boolean leftMatched;

        /** */
        protected boolean drainMaterialization;

        /** */
        protected Iterator<RowList> materializedIt;

        /**
         * Constructor.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory (or {@code null} if join don't produce rows for unmatched right side).
         * @param rightRowFactory Right row factory (or {@code null} if join don't produce rows for unmatched left side).
         */
        private AbstractMatchingHashJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<Row> outRowHnd,
            @Nullable RowHandler.RowFactory<Row> leftRowFactory,
            @Nullable RowHandler.RowFactory<Row> rightRowFactory,
            @Nullable BiPredicate<Row, Row> nonEqCond
        ) {
            super(ctx, rowType, info, leftRowFactory != null, nonEqCond);

            outRowFactory = outRowHnd::concat;
            emptyLeftRow = leftRowFactory == null ? null : leftRowFactory.create();
            emptyRightRow = rightRowFactory == null ? null : rightRowFactory.create();
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            drainMaterialization = false;

            leftMatched = false;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (checkNextLeftRowStarted())
                            leftMatched = false;

                        while (hasNextRight()) {
                            if (rescheduleJoin())
                                return;

                            Row right = nextRight();

                            if (nonEqCond != null && !nonEqCond.test(left, right))
                                continue;

                            leftMatched = true;

                            // Emit matched row.
                            downstream().push(outRowFactory.apply(left, right));

                            touchRight();

                            if (--requested == 0)
                                return;
                        }

                        assert requested > 0;

                        // For LEFT/FULL join.
                        if (emptyRightRow != null && !leftMatched) {
                            // Emit unmatched left row.
                            requested--;

                            downstream().push(outRowFactory.apply(left, emptyRightRow));
                        }

                        left = null;
                    }

                    // For RIGHT/FULL join.
                    if (emptyLeftRow != null && leftFinished() && requested > 0) {
                        // Emit unmatched right rows.
                        if (!hasNextRight() && !drainMaterialization) {
                            // Prevent scanning store more than once.
                            drainMaterialization = true;

                            materializedIt = hashStore.values().iterator();
                        }

                        while (requested > 0 && hasNextRight()) {
                            if (rescheduleJoin())
                                return;

                            Row right = nextRight();

                            if (touchedRight())
                                continue;

                            Row row = outRowFactory.apply(emptyLeftRow, right);

                            --requested;

                            downstream().push(row);
                        }
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
        @Override protected boolean hasNextRight() {
            boolean res = super.hasNextRight();

            if (drainMaterialization && !res && materializedIt.hasNext()) {
                rightRows = materializedIt.next();
                rightIdx = 0;
                return true; // Every key in hashStore contains at least one row.
            }

            return res;
        }

        /** */
        protected void touchRight() {
            // No-op.
        }

        /** */
        protected boolean touchedRight() {
            return false;
        }
    }

    /** */
    private abstract static class AbstractNoTouchingHashJoin<RowT>
        extends AbstractMatchingHashJoin<RowT, ArrayList<RowT>> {
        /**
         * Constructor.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private AbstractNoTouchingHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable RowHandler.RowFactory<RowT> leftRowFactory,
            @Nullable RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, leftRowFactory, rightRowFactory, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected ArrayList<RowT> createRowList() {
            return new ArrayList<>();
        }
    }

    /** */
    private static final class InnerHashJoin<RowT> extends AbstractNoTouchingHashJoin<RowT> {
        /**
         * Creates node for INNER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private InnerHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, null, null, nonEqCond);
        }
    }

    /** */
    private static final class LeftHashJoin<RowT> extends AbstractNoTouchingHashJoin<RowT> {
        /**
         * Creates node for LEFT OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param info Join info.
         * @param rowType Out row type.
         * @param outRowHnd Output row handler.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private LeftHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, null, rightRowFactory, nonEqCond);
        }
    }

    /** */
    private abstract static class AbstractListTouchingHashJoin<RowT>
        extends AbstractMatchingHashJoin<RowT, TouchableList<RowT>> {
        /**
         * Constructor.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private AbstractListTouchingHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable RowHandler.RowFactory<RowT> leftRowFactory,
            @Nullable RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, leftRowFactory, rightRowFactory, nonEqCond);

            assert nonEqCond == null;
        }

        /** {@inheritDoc} */
        @Override protected TouchableList<RowT> createRowList() {
            return new TouchableList<>();
        }

        /** {@inheritDoc} */
        @Override protected @Nullable TouchableList<RowT> lookup(RowT t) {
            TouchableList<RowT> res = super.lookup(t);

            if (res != null)
                res.touch();

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean touchedRight() {
            return rightRows.touched();
        }

        /** {@inheritDoc} */
        @Override protected boolean hasNextRight() {
            boolean res = super.hasNextRight();

            if (drainMaterialization && res && rightIdx > 0 && rightRows.touched()) {
                // Emit one row even for touched list, to correctly reschedule after some rows have been processed
                // and allow others to do their job.
                if (materializedIt.hasNext()) {
                    rightRows = materializedIt.next();
                    rightIdx = 0;
                    return true; // Every key in hashStore contains at least one row.
                }

                return false;
            }

            return res;
        }
    }

    /** */
    private static class RightHashJoin<RowT> extends AbstractListTouchingHashJoin<RowT> {
        /**
         * Creates node for RIGHT OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param leftRowFactory Left row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private RightHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, leftRowFactory, null, nonEqCond);
        }
    }

    /** */
    private static final class FullOuterHashJoin<RowT> extends AbstractListTouchingHashJoin<RowT> {
        /**
         * Creates node for FULL OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
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
            super(ctx, rowType, info, outRowHnd, leftRowFactory, rightRowFactory, nonEqCond);
        }
    }

    /** */
    private abstract static class AbstractRowTouchingHashJoin<RowT>
        extends AbstractMatchingHashJoin<RowT, RowTouchableList<RowT>> {
        /**
         * Constructor.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private AbstractRowTouchingHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            @Nullable RowHandler.RowFactory<RowT> leftRowFactory,
            @Nullable RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, leftRowFactory, rightRowFactory, nonEqCond);

            assert nonEqCond != null;
        }

        /** {@inheritDoc} */
        @Override protected RowTouchableList<RowT> createRowList() {
            return new RowTouchableList<>();
        }

        /** {@inheritDoc} */
        @Override protected void touchRight() {
            // Index was already moved by nextRight call, so use previous index.
            rightRows.touch(rightIdx - 1);
        }

        /** {@inheritDoc} */
        @Override protected boolean touchedRight() {
            // Index was already moved by nextRight call, so use previous index.
            return rightRows.touched(rightIdx - 1);
        }
    }

    /** */
    private static class RightRowTouchingHashJoin<RowT> extends AbstractRowTouchingHashJoin<RowT> {
        /**
         * Creates node for RIGHT OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param leftRowFactory Left row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private RightRowTouchingHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, leftRowFactory, null, nonEqCond);
        }
    }

    /** */
    private static final class FullOuterRowTouchingHashJoin<RowT> extends AbstractRowTouchingHashJoin<RowT> {
        /**
         * Creates node for FULL OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Out row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private FullOuterRowTouchingHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory,
            RowHandler.RowFactory<RowT> rightRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, outRowHnd, leftRowFactory, rightRowFactory, nonEqCond);
        }
    }

    /**
     * Abstract base class for filtering rows of the left table based on matching or non-matching conditions with the
     * right table.
     */
    private abstract static class AbstractFilteringHashJoin<RowT> extends AbstractStoringHashJoin<RowT, ArrayList<RowT>> {
        /**
         * Constructor.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param nonEqCond Non-equi conditions.
         */
        private AbstractFilteringHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, false, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkNextLeftRowStarted();

                        boolean anyMatched = hasNextRight() && nonEqCond == null;

                        if (!anyMatched) {
                            // Find any matched right row.
                            while (hasNextRight()) {
                                RowT right = nextRight();

                                if (nonEqCond.test(left, right)) {
                                    anyMatched = true;

                                    break;
                                }

                                if (rescheduleJoin())
                                    return;
                            }
                        }

                        if (anyMatched) {
                            onMatchesFound();

                            rightRows = null;
                        }
                        else
                            onMatchesNotFound();

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

        /** {@inheritDoc} */
        @Override protected ArrayList<RowT> createRowList() {
            return new ArrayList<>();
        }

        /** Triggered when matches found for current left row. */
        protected void onMatchesFound() throws Exception {
            // No-op.
        }

        /** Triggered when matches not found for current left row. */
        protected void onMatchesNotFound() throws Exception {
            // No-op.
        }
    }

    /** */
    private static final class SemiHashJoin<RowT> extends AbstractFilteringHashJoin<RowT> {
        /**
         * Creates node for SEMI JOIN operator.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param nonEqCond If provided, only rows matching the predicate will be emitted as matched rows.
         */
        private SemiHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected void onMatchesFound() throws Exception {
            requested--;

            downstream().push(left);
        }
    }

    /** */
    private static final class AntiHashJoin<RowT> extends AbstractFilteringHashJoin<RowT> {
        /**
         * Creates node for ANTI JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Out row type.
         * @param info Join info.
         * @param nonEqCond Non-equi conditions.
         */
        private AntiHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteJoinInfo info,
            @Nullable BiPredicate<RowT, RowT> nonEqCond
        ) {
            super(ctx, rowType, info, nonEqCond);
        }

        /** {@inheritDoc} */
        @Override protected void onMatchesNotFound() throws Exception {
            requested--;

            downstream().push(left);
        }
    }

    /** */
    private static final class TouchableList<T> extends ArrayList<T> {
        /** */
        private boolean touched;

        /** */
        public void touch() {
            touched = true;
        }

        /** */
        public boolean touched() {
            return touched;
        }
    }

    /** */
    private static final class RowTouchableList<T> extends ArrayList<T> {
        /** */
        private @Nullable BitSet touched;

        /** */
        public void touch(int idx) {
            if (touched == null)
                touched = new BitSet(size());

            touched.set(idx);
        }

        /** */
        public boolean touched(int idx) {
            if (touched == null)
                return false;

            return touched.get(idx);
        }
    }
}
