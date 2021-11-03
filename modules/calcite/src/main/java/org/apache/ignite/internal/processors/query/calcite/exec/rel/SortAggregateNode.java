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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class SortAggregateNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    /**
     *
     */
    private final AggregateType type;

    /** May be {@code null} when there are not accumulators (DISTINCT aggregate node). */
    private final Supplier<List<AccumulatorWrapper<RowT>>> accFactory;

    /**
     *
     */
    private final RowFactory<RowT> rowFactory;

    /**
     *
     */
    private final ImmutableBitSet grpSet;

    /**
     *
     */
    private final Comparator<RowT> comp;

    /**
     *
     */
    private final Deque<RowT> outBuf = new ArrayDeque<>(inBufSize);

    /**
     *
     */
    private RowT prevRow;

    /**
     *
     */
    private Group grp;

    /**
     *
     */
    private int requested;

    /**
     *
     */
    private int waiting;

    /**
     *
     */
    private int cmpRes;

    /**
     * @param ctx Execution context.
     */
    public SortAggregateNode(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            AggregateType type,
            ImmutableBitSet grpSet,
            Supplier<List<AccumulatorWrapper<RowT>>> accFactory,
            RowFactory<RowT> rowFactory,
            Comparator<RowT> comp
    ) {
        super(ctx, rowType);
        assert Objects.nonNull(comp);

        this.type = type;
        this.accFactory = accFactory;
        this.rowFactory = rowFactory;
        this.grpSet = grpSet;
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!outBuf.isEmpty()) {
            doPush();
        }

        if (waiting == 0) {
            waiting = inBufSize;

            source().request(inBufSize);
        } else if (waiting < 0) {
            downstream().end();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        if (grp != null) {
            int cmp = comp.compare(row, prevRow);

            if (cmp == 0) {
                grp.add(row);
            } else {
                if (cmpRes == 0) {
                    cmpRes = cmp;
                } else {
                    assert Integer.signum(cmp) == Integer.signum(cmpRes) : "Input not sorted";
                }

                outBuf.add(grp.row());

                grp = newGroup(row);

                doPush();
            }
        } else {
            grp = newGroup(row);
        }

        prevRow = row;

        if (waiting == 0 && requested > 0) {
            waiting = inBufSize;

            context().execute(() -> source().request(inBufSize), this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        if (grp != null) {
            outBuf.add(grp.row());

            doPush();
        }

        if (requested > 0) {
            downstream().end();
        }

        grp = null;
        prevRow = null;
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        grp = null;
        prevRow = null;
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    /**
     *
     */
    private boolean hasAccumulators() {
        return accFactory != null;
    }

    /**
     *
     */
    private Group newGroup(RowT r) {
        final Object[] grpKeys = new Object[grpSet.cardinality()];
        List<Integer> fldIdxs = grpSet.asList();

        final RowHandler<RowT> rowHandler = rowFactory.handler();

        for (int i = 0; i < grpKeys.length; ++i) {
            grpKeys[i] = rowHandler.get(fldIdxs.get(i), r);
        }

        Group grp = new Group(grpKeys);

        grp.add(r);

        return grp;
    }

    /**
     *
     */
    private void doPush() throws Exception {
        while (requested > 0 && !outBuf.isEmpty()) {
            requested--;

            downstream().push(outBuf.poll());
        }
    }

    /**
     *
     */
    private class Group {
        /**
         *
         */
        private final List<AccumulatorWrapper<RowT>> accumWrps;

        /**
         *
         */
        private final RowHandler<RowT> handler;

        /**
         *
         */
        private final Object[] grpKeys;

        /**
         *
         */
        private Group(Object[] grpKeys) {
            this.grpKeys = grpKeys;

            accumWrps = hasAccumulators() ? accFactory.get() : Collections.emptyList();

            handler = context().rowHandler();
        }

        /**
         *
         */
        private void add(RowT row) {
            if (type == AggregateType.REDUCE) {
                addOnReducer(row);
            } else {
                addOnMapper(row);
            }
        }

        /**
         *
         */
        private RowT row() {
            if (type == AggregateType.MAP) {
                return rowOnMapper();
            } else {
                return rowOnReducer();
            }
        }

        /**
         *
         */
        private void addOnMapper(RowT row) {
            for (AccumulatorWrapper<RowT> wrapper : accumWrps) {
                wrapper.add(row);
            }
        }

        /**
         *
         */
        private void addOnReducer(RowT row) {
            List<Accumulator> accums = hasAccumulators()
                    ? (List<Accumulator>) handler.get(handler.columnCount(row) - 1, row) : Collections.emptyList();

            for (int i = 0; i < accums.size(); i++) {
                AccumulatorWrapper<RowT> wrapper = accumWrps.get(i);

                Accumulator accum = accums.get(i);

                wrapper.apply(accum);
            }
        }

        /**
         *
         */
        private RowT rowOnMapper() {
            Object[] fields = new Object[grpSet.cardinality() + (accFactory != null ? 1 : 0)];

            int i = 0;

            for (Object grpKey : grpKeys) {
                fields[i++] = grpKey;
            }

            // Last column is the accumulators collection.
            if (hasAccumulators()) {
                fields[i] = Commons.transform(accumWrps, AccumulatorWrapper::accumulator);
            }

            return rowFactory.create(fields);
        }

        /**
         *
         */
        private RowT rowOnReducer() {
            Object[] fields = new Object[grpSet.cardinality() + accumWrps.size()];

            int i = 0;

            for (Object grpKey : grpKeys) {
                fields[i++] = grpKey;
            }

            for (AccumulatorWrapper<RowT> accWrp : accumWrps) {
                fields[i++] = accWrp.end();
            }

            return rowFactory.create(fields);
        }
    }
}
