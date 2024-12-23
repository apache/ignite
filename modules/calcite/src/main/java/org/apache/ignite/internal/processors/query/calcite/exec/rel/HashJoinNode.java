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
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/** HashJoin implementor. */
public abstract class HashJoinNode<Row> extends AbstractRightMaterializedJoinNode<Row> {
    /** */
    private static final int INITIAL_CAPACITY = 128;

    /** All keys with null-fields are mapped to this object. */
    private static final Key NULL_KEY = new Key();

    /** */
    private final int[] leftJoinPositions;

    /** */
    private final int[] rightJoinPositions;

    /** Output row handler. */
    protected final RowHandler<Row> outRowHnd;

    /** */
    protected final Map<Key, TouchedCollection<Row>> hashStore = new HashMap<>(INITIAL_CAPACITY);

    /** */
    protected Iterator<Row> rightIt = Collections.emptyIterator();

    /**
     * Creates hash join node.
     *
     * @param ctx Execution context.
     * @param rowType Row type.
     * @param info Join info.
     * @param outRowHnd Output row handler.
     */
    protected HashJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, JoinInfo info, RowHandler<Row> outRowHnd) {
        super(ctx, rowType);

        leftJoinPositions = info.leftKeys.toIntArray();
        rightJoinPositions = info.rightKeys.toIntArray();

        assert leftJoinPositions.length == rightJoinPositions.length;

        this.outRowHnd = outRowHnd;
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        rightIt = Collections.emptyIterator();

        hashStore.clear();

        super.rewindInternal();
    }

    /** Creates certain join node. */
    public static <RowT> HashJoinNode<RowT> create(
        ExecutionContext<RowT> ctx,
        RelDataType outRowType,
        RelDataType leftRowType,
        RelDataType rightRowType,
        JoinRelType type,
        JoinInfo info
    ) {
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        RowHandler<RowT> rowHnd = ctx.rowHandler();

        switch (type) {
            case INNER:
                return new InnerHashJoin<>(ctx, outRowType, info, rowHnd);

            case LEFT:
                return new LeftHashJoin<>(ctx, outRowType, info, rowHnd, rowHnd.factory(typeFactory, rightRowType));

            case RIGHT:
                return new RightHashJoin<>(ctx, outRowType, info, rowHnd, rowHnd.factory(typeFactory, leftRowType));

            case FULL:
                return new FullOuterHashJoin<>(ctx, outRowType, info, rowHnd, rowHnd.factory(typeFactory, leftRowType),
                    rowHnd.factory(typeFactory, rightRowType));

            case SEMI:
                return new SemiHashJoin<>(ctx, outRowType, info, rowHnd);

            case ANTI:
                return new AntiHashJoin<>(ctx, outRowType, info, rowHnd);

            default:
                throw new IllegalArgumentException("Join of type ''" + type + "' isn't supported yet.");
        }
    }

    /** */
    protected Collection<Row> lookup(Row row) {
        Key row0 = extractKey(row, leftJoinPositions);

        // Key with null field can't be compared with other keys.
        if (row0 == NULL_KEY)
            return Collections.emptyList();

        TouchedCollection<Row> found = hashStore.get(row0);

        if (found != null) {
            found.touched = true;

            return found.items();
        }

        return Collections.emptyList();
    }

    /** */
    private static <RowT> Iterator<RowT> untouched(Map<Key, TouchedCollection<RowT>> entries) {
        return new Iterator<RowT>() {
            private final Iterator<TouchedCollection<RowT>> it = entries.values().iterator();

            private Iterator<RowT> innerIt = Collections.emptyIterator();

            @Override public boolean hasNext() {
                if (innerIt.hasNext())
                    return true;

                advance();

                return innerIt.hasNext();
            }

            @Override public RowT next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return innerIt.next();
            }

            void advance() {
                while (it.hasNext()) {
                    TouchedCollection<RowT> coll = it.next();

                    if (!coll.touched && !coll.items().isEmpty()) {
                        innerIt = coll.items().iterator();

                        break;
                    }
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        nodeMemoryTracker.onRowAdded(row);

        waitingRight--;

        Key key = extractKey(row, rightJoinPositions);

        // No storing the row in hashStore, if it contains NULL. And we won't emit right part alone like in FULL OUTER and RIGHT joins.
        if (keepRowsWithNull() || key != NULL_KEY) {
            TouchedCollection<Row> raw = hashStore.computeIfAbsent(key, k -> new TouchedCollection<>());

            raw.add(row);
        }

        if (waitingRight == 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }

    /** */
    private Key extractKey(Row row, int[] mapping) {
        RowHandler<Row> rowHnd = context().rowHandler();

        for (int i : mapping) {
            if (rowHnd.get(i, row) == null)
                return NULL_KEY;
        }

        return new RowWrapper<>(row, rowHnd, mapping);
    }

    /** */
    protected void getMoreOrEnd() throws Exception {
        if (waitingRight == 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);

        if (waitingLeft == 0 && leftInBuf.isEmpty())
            leftSource().request(waitingLeft = IN_BUFFER_SIZE);

        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty() && left == null
                && !rightIt.hasNext()) {
            requested = 0;

            downstream().end();
        }
    }

    /**
     * Returns {@code true} if we need to store the row from right shoulder even if it contains NULL in any of join key position.
     *
     * <p>This is required for joins which emit unmatched part of the right shoulder, such as RIGHT JOIN and FULL OUTER JOIN.
     *
     * @return {@code true} when row must be stored in {@link #hashStore} unconditionally.
     */
    protected boolean keepRowsWithNull() {
        return false;
    }

    /** */
    private static final class InnerHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates node for INNER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Output row factory.
         */
        private InnerHashJoin(ExecutionContext<RowT> ctx, RelDataType rowType, JoinInfo info, RowHandler<RowT> outRowHnd) {
            super(ctx, rowType, info, outRowHnd);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            rightIt = lookup(left).iterator();
                        }

                        while (rightIt.hasNext()) {
                            checkState();

                            RowT right = rightIt.next();

                            --requested;

                            downstream().push(outRowHnd.concat(left, right));

                            if (requested == 0)
                                break;
                        }

                        if (!rightIt.hasNext())
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
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
         * @param rowType Row tyoe.
         * @param outRowHnd Output row handler.
         * @param rightRowFactory Right row factory.
         */
        private LeftHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            JoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, rowType, info, outRowHnd);

            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            if (rightRows.isEmpty()) {
                                requested--;

                                downstream().push(outRowHnd.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        while (rightIt.hasNext()) {
                            checkState();

                            RowT right = rightIt.next();

                            --requested;

                            downstream().push(outRowHnd.concat(left, right));

                            if (requested == 0)
                                break;
                        }

                        if (!rightIt.hasNext())
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    /** */
    private static final class RightHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /**
         * Creates node for RIGHT OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Output row factory.
         * @param leftRowFactory Left row factory.
         */
        private RightHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            JoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, rowType, info, outRowHnd);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            rightIt = lookup(left).iterator();
                        }

                        while (rightIt.hasNext()) {
                            checkState();

                            RowT right = rightIt.next();

                            --requested;

                            downstream().push(outRowHnd.concat(left, right));

                            if (requested == 0)
                                break;
                        }

                        if (!rightIt.hasNext())
                            left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (left == null && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;

                try {
                    if (!rightIt.hasNext())
                        rightIt = untouched(hashStore);

                    RowT emptyLeft = leftRowFactory.create();

                    while (rightIt.hasNext()) {
                        checkState();

                        RowT right = rightIt.next();

                        RowT row = outRowHnd.concat(emptyLeft, right);

                        --requested;

                        downstream().push(row);

                        if (requested == 0)
                            break;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }

        /** {@inheritDoc} */
        @Override protected boolean keepRowsWithNull() {
            return true;
        }
    }

    /** */
    private static class FullOuterHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /**
         * Creates node for FULL OUTER JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        private FullOuterHashJoin(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            JoinInfo info,
            RowHandler<RowT> outRowHnd,
            RowHandler.RowFactory<RowT> leftRowFactory,
            RowHandler.RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, rowType, info, outRowHnd);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            if (rightRows.isEmpty()) {
                                requested--;

                                downstream().push(outRowHnd.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        while (rightIt.hasNext()) {
                            checkState();

                            RowT right = rightIt.next();

                            --requested;

                            downstream().push(outRowHnd.concat(left, right));

                            if (requested == 0)
                                break;
                        }

                        if (!rightIt.hasNext())
                            left = null;

                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (left == null && !rightIt.hasNext() && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING
                && requested > 0) {
                inLoop = true;

                try {
                    if (!rightIt.hasNext())
                        rightIt = untouched(hashStore);

                    RowT emptyLeft = leftRowFactory.create();

                    while (rightIt.hasNext()) {
                        checkState();

                        RowT row = outRowHnd.concat(emptyLeft, rightIt.next());

                        --requested;

                        downstream().push(row);

                        if (requested == 0)
                            break;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }

        /** {@inheritDoc} */
        @Override protected boolean keepRowsWithNull() {
            return true;
        }
    }

    /** */
    private static final class SemiHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates node for SEMI JOIN operator.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         */
        private SemiHashJoin(ExecutionContext<RowT> ctx, RelDataType rowType, JoinInfo info, RowHandler<RowT> outRowHnd) {
            super(ctx, rowType, info, outRowHnd);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        left = leftInBuf.remove();

                        Collection<RowT> rightRows = lookup(left);

                        if (!rightRows.isEmpty()) {
                            requested--;

                            downstream().push(left);

                            if (requested == 0)
                                break;
                        }

                        left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    /** */
    private static final class AntiHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates node for ANTI JOIN.
         *
         * @param ctx Execution context.
         * @param rowType Row type.
         * @param info Join info.
         * @param outRowHnd Output row handler.
         */
        private AntiHashJoin(ExecutionContext<RowT> ctx, RelDataType rowType, JoinInfo info, RowHandler<RowT> outRowHnd) {
            super(ctx, rowType, info, outRowHnd);
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        left = leftInBuf.remove();

                        Collection<RowT> rightRows = lookup(left);

                        if (rightRows.isEmpty()) {
                            requested--;

                            downstream().push(left);

                            if (requested == 0)
                                break;
                        }

                        left = null;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    /** Non-comparable key object. */
    private static class Key {

    }

    /** Comparable key object. */
    private static final class RowWrapper<RowT> extends Key {
        /** */
        private final RowT row;

        /** */
        private final RowHandler<RowT> handler;

        /** */
        private final int[] items;

        /** */
        private RowWrapper(RowT row, RowHandler<RowT> hnd, int[] items) {
            this.row = row;
            this.handler = hnd;
            this.items = items;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int hashCode = 0;

            for (int i : items)
                hashCode += Objects.hashCode(handler.get(i, row));

            return hashCode;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            RowWrapper<RowT> row0 = (RowWrapper<RowT>)obj;

            for (int i = 0; i < items.length; ++i) {
                Object input = row0.handler.get(row0.items[i], row0.row);
                Object cur = handler.get(items[i], row);

                boolean comp = Objects.equals(input, cur);

                if (!comp)
                    return comp;
            }

            return true;
        }
    }

    /** */
    private static final class TouchedCollection<RowT> {
        /** */
        private final Collection<RowT> coll;

        /** */
        private boolean touched;

        /** */
        private TouchedCollection() {
            this.coll = new ArrayList<>();
        }

        /** */
        private void add(RowT row) {
            coll.add(row);
        }

        /** */
        private Collection<RowT> items() {
            return coll;
        }
    }
}
