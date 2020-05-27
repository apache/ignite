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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class AggregateNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final AggregateType type;

    /** */
    private final Supplier<List<AccumulatorWrapper<Row>>> wrappersFactory;

    /** */
    private final RowFactory<Row> rowFactory;

    /** */
    private final ImmutableBitSet grpSet;

    /** */
    private final List<Grouping> groupings;

    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     */
    public AggregateNode(ExecutionContext<Row> ctx, AggregateType type, List<ImmutableBitSet> grpSets,
        Supplier<List<AccumulatorWrapper<Row>>> wrappersFactory, RowFactory<Row> rowFactory) {
        super(ctx);

        this.type = type;
        this.wrappersFactory = wrappersFactory;
        this.rowFactory = rowFactory;

        ImmutableBitSet.Builder b = ImmutableBitSet.builder();

        if (grpSets.size() > Byte.MAX_VALUE)
            throw new IgniteException("Too many groups");

        groupings = new ArrayList<>(grpSets.size());

        for (byte i = 0; i < grpSets.size(); i++) {
            ImmutableBitSet grpFields = grpSets.get(i);
            groupings.add(new Grouping(i, grpFields));

            b.addAll(grpFields);
        }

        grpSet = b.build();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (waiting == -1 && !inLoop)
            context().execute(this::flushFromBuffer);
        else if (waiting == 0)
            F.first(sources).request(waiting = IN_BUFFER_SIZE);
        else
            throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting--;

        try {
            for (Grouping grouping : groupings)
                grouping.add(row);

            if (waiting == 0)
                F.first(sources).request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting = -1;

        try {
            flushFromBuffer();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    public void flushFromBuffer() {
        assert waiting == -1;

        inLoop = true;
        try {
            int processed = 0;

            ArrayDeque<Grouping> groupingsQueue = groupingsQueue();

            while (requested > 0 && !groupingsQueue.isEmpty()) {
                Grouping grouping = groupingsQueue.peek();

                int toSnd = Math.min(requested, IN_BUFFER_SIZE - processed);

                for (Row row : grouping.getRows(toSnd)) {
                    requested--;
                    downstream.push(row);

                    processed++;
                }

                if (processed >= IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flushFromBuffer);

                    return;
                }

                if (grouping.isEmpty())
                    groupingsQueue.remove();
            }

            if (requested > 0) {
                downstream.end();
                requested = 0;
            }
        }
        finally {
            inLoop = false;
        }
    }

    /** */
    private ArrayDeque<Grouping> groupingsQueue() {
        ArrayDeque<Grouping> res = new ArrayDeque<>(groupings.size());

        for (Grouping grouping : groupings) {
            if (!grouping.isEmpty())
                res.add(grouping);
        }

        return res;
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public enum AggregateType {
        /** Map phase. */
        MAP,

        /** Reduce phase. */
        REDUCE,

        /** Single phase aggregate. */
        SINGLE
    }

    /** */
    private class Grouping {
        /** */
        private final byte grpId;

        /** */
        private final ImmutableBitSet grpFields;

        /** */
        private final Map<GroupKey, List<AccumulatorWrapper<Row>>> groups = new HashMap<>();

        /** */
        private final RowHandler<Row> handler;

        /** */
        private Grouping(byte grpId, ImmutableBitSet grpFields) {
            this.grpId = grpId;
            this.grpFields = grpFields;

            handler = ctx.rowHandler();
        }

        /** */
        private void add(Row row) {
            if (type == AggregateType.REDUCE)
                addOnReducer(row);
            else
                addOnMapper(row);
        }

        /**
         * @param cnt Number of rows.
         *
         * @return Actually sent rows number.
         */
        private List<Row> getRows(int cnt) {
            if (F.isEmpty(groups))
                return Collections.emptyList();
            else if (type == AggregateType.MAP)
                return getOnMapper(cnt);
            else
                return getOnReducer(cnt);
        }

        /** */
        private void addOnMapper(Row row) {
            GroupKey.Builder b = GroupKey.builder(grpFields.cardinality());

            for (Integer field : grpFields)
                b.add(handler.get(field, row));

            GroupKey grpKey = b.build();

            List<AccumulatorWrapper<Row>> wrappers = groups.computeIfAbsent(grpKey, this::create);

            for (AccumulatorWrapper<Row> wrapper : wrappers)
                wrapper.add(row);
        }

        /** */
        private void addOnReducer(Row row) {
            byte targetGrpId = (byte)handler.get(0, row);

            if (targetGrpId != grpId)
                return;

            GroupKey grpKey = (GroupKey)handler.get(1, row);

            List<AccumulatorWrapper<Row>> wrappers = groups.computeIfAbsent(grpKey, this::create);
            List<Accumulator> accums = (List<Accumulator>)handler.get(2, row);

            for (int i = 0; i < wrappers.size(); i++) {
                AccumulatorWrapper<Row> wrapper = wrappers.get(i);
                Accumulator accum = accums.get(i);

                wrapper.apply(accum);
            }
        }

        /** */
        private List<Row> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey, List<AccumulatorWrapper<Row>>>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<Row> res = new ArrayList<>(amount);

            for (int i = 0; i < amount; i++) {
                Map.Entry<GroupKey, List<AccumulatorWrapper<Row>>> entry = it.next();

                GroupKey grpKey = entry.getKey();
                List<Accumulator> accums = Commons.transform(entry.getValue(), AccumulatorWrapper::accumulator);

                res.add(rowFactory.create(grpId, grpKey, accums));
                it.remove();
            }

            return res;
        }

        /** */
        private List<Row> getOnReducer(int cnt) {
            Iterator<Map.Entry<GroupKey, List<AccumulatorWrapper<Row>>>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<Row> res = new ArrayList<>(amount);

            for (int i = 0; i < amount; i++) {
                Map.Entry<GroupKey, List<AccumulatorWrapper<Row>>> entry = it.next();

                GroupKey grpKey = entry.getKey();
                List<AccumulatorWrapper<Row>> wrappers = entry.getValue();

                Object[] fields = new Object[grpSet.cardinality() + wrappers.size()];

                int j = 0, k = 0;

                for (Integer field : grpSet)
                    fields[j++] = grpFields.get(field) ? grpKey.field(k++) : null;

                for (AccumulatorWrapper<Row> wrapper : wrappers)
                    fields[j++] = wrapper.end();

                res.add(rowFactory.create(fields));
                it.remove();
            }

            return res;
        }

        /** */
        private List<AccumulatorWrapper<Row>> create(GroupKey key) {
            return wrappersFactory.get();
        }

        /** */
        private boolean isEmpty() {
            return groups.isEmpty();
        }
    }
}
