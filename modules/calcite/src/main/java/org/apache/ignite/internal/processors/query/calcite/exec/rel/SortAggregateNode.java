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

import java.util.Comparator;
import java.util.List;
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
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class SortAggregateNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final AggregateType type;

    /** */
    private final Supplier<List<AccumulatorWrapper<Row>>> accFactory;

    /** */
    private final RowFactory<Row> rowFactory;

    /** */
    private final ImmutableBitSet grpSet;

    /** */
    private final Comparator<Row> comp;

    /** */
    private Row prevRow;

    /** */
    private Group grp;

    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private int cmpRes;

    /**
     * @param ctx Execution context.
     */
    public SortAggregateNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        AggregateType type,
        ImmutableBitSet grpSet,
        Supplier<List<AccumulatorWrapper<Row>>> accFactory,
        RowFactory<Row> rowFactory,
        Comparator<Row> comp
    ) {
        super(ctx, rowType);

        this.type = type;
        this.accFactory = accFactory;
        this.rowFactory = rowFactory;
        this.grpSet = grpSet;
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        try {
            checkState();

            requested = rowsCnt;

            if (waiting == 0)
                source().request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        assert downstream() != null;
        assert waiting > 0;

        try {
            checkState();

            waiting--;

            if (grp != null) {
                int cmp = comp.compare(row, prevRow);

                if (cmp == 0)
                    grp.add(row);
                else {
                    if (cmpRes == 0)
                        cmpRes = cmp;
                    else
                        assert cmp == cmpRes : "Input not sorted";

                    doPush();

                    grp = newGroup(row);
                }
            }
            else
                grp = newGroup(row);

            prevRow = row;

            if (waiting == 0)
                source().request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        assert downstream() != null;
        assert waiting > 0;

        try {
            checkState();

            waiting = -1;

            if (grp != null)
                doPush();

            grp = null;
            prevRow = null;

            downstream().end();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        grp = null;
        prevRow = null;
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    private Group newGroup(Row r) {
        final Object[] grpKeys = new Object[grpSet.cardinality()];
        List<Integer> fldIdxs = grpSet.asList();

        final RowHandler<Row> rowHandler = rowFactory.handler();

        for (int i = 0; i < grpKeys.length; ++i)
            grpKeys[i] = rowHandler.get(fldIdxs.get(i), r);

        Group grp = new Group(grpKeys);

        grp.add(r);

        return grp;
    }

    /** */
    private void doPush() throws Exception {
        requested--;

        downstream().push(grp.row());
    }

    /** */
    private class Group {
        /** */
        private final List<AccumulatorWrapper<Row>> accumWrps;

        /** */
        private final RowHandler<Row> handler;

        /** */
        private final Object[] grpKeys;

        /** */
        private Group(Object[] grpKeys) {
            this.grpKeys = grpKeys;

            accumWrps = accFactory.get();

            handler = context().rowHandler();
        }

        /** */
        private void add(Row row) {
            if (type == AggregateType.REDUCE)
                addOnReducer(row);
            else
                addOnMapper(row);
        }

        /** */
        private Row row() {
            if (type == AggregateType.MAP)
                return rowOnMapper();
            else
                return rowOnReducer();
        }

        /** */
        private void addOnMapper(Row row) {
            for (AccumulatorWrapper<Row> wrapper : accumWrps)
                wrapper.add(row);
        }

        /** */
        private void addOnReducer(Row row) {
            List<Accumulator> accums = (List<Accumulator>)handler.get(handler.columnCount(row) - 1, row);

            for (int i = 0; i < accums.size(); i++) {
                AccumulatorWrapper<Row> wrapper = accumWrps.get(i);

                Accumulator accum = accums.get(i);

                wrapper.apply(accum);
            }
        }

        /** */
        private Row rowOnMapper() {
            Object[] fields = new Object[grpSet.cardinality() + 1];

            int i = 0;

            for (Object grpKey : grpKeys)
                fields[i++] = grpKey;

            // Last column is the accumulators collection.
            fields[i] = Commons.transform(accumWrps, AccumulatorWrapper::accumulator);

            return rowFactory.create(fields);
        }

        /** */
        private Row rowOnReducer() {
            Object[] fields = new Object[grpSet.cardinality() + accumWrps.size()];

            int i = 0;

            for (Object grpKey : grpKeys)
                fields[i++] = grpKey;

            for (AccumulatorWrapper<Row> accWrp : accumWrps)
                fields[i++] = accWrp.end();

            return rowFactory.create(fields);
        }
    }
}
