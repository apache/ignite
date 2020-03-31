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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class AggregateNode<T> extends AbstractNode<T> implements SingleNode<T>, Downstream<T> {
    /** */
    private final AggregateType type;

    /** */
    private final Supplier<List<AccumulatorWrapper>> wrappersFactory;

    /** */
    private final RowHandler<T> handler;

    /** */
    private final ImmutableBitSet groupSet;

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
    public AggregateNode(ExecutionContext ctx, AggregateType type, List<ImmutableBitSet> groupSets,
        Supplier<List<AccumulatorWrapper>> wrappersFactory, RowHandler<T> handler) {
        super(ctx);

        this.type = type;
        this.wrappersFactory = wrappersFactory;
        this.handler = handler;

        ImmutableBitSet.Builder b = ImmutableBitSet.builder();

        if (groupSets.size() > Byte.MAX_VALUE)
            throw new IgniteException("Too many groups");

        groupings = new ArrayList<>(groupSets.size());

        for (byte i = 0; i < groupSets.size(); i++) {
            ImmutableBitSet groupFields = groupSets.get(i);
            groupings.add(new Grouping(i, groupFields));

            b.addAll(groupFields);
        }

        groupSet = b.build();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCount > 0 && requested == 0;

        requested = rowsCount;

        if (waiting == -1 && !inLoop)
            context().execute(this::flushFromBuffer);
        else if (waiting == 0)
            F.first(sources).request(waiting = IN_BUFFER_SIZE);
        else
            throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public void push(T row) {
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
    @Override protected Downstream<T> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    @SuppressWarnings({"LabeledStatement", "ContinueStatementWithLabel"})
    public void flushFromBuffer() {
        assert waiting == -1;

        inLoop = true;
        try {
            int processed = 0;

            parent:
            while (requested > 0) {
                for (Grouping group : groupings) {
                    int request = requested;
                    requested = 0; // not to violate assert in request() method

                    int sent = group.send(downstream, request);

                    if ((processed += sent) >= IN_BUFFER_SIZE && requested > 0) {
                        // allow others to do their job
                        context().execute(this::flushFromBuffer);

                        return;
                    }

                    if (request == sent)
                        continue parent;

                    assert requested == 0;

                    requested = request - sent;
                }

                if (requested > 0) {
                    downstream.end();
                    requested = 0;
                }
            }
        }
        finally {
            inLoop = false;
        }
    }

    /** */
    public enum AggregateType {
        MAP, REDUCE, SINGLE
    }

    /** */
    private class Grouping {
        /** */
        private final byte groupId;

        /** */
        private final ImmutableBitSet groupFields;

        /** */
        private final Map<GroupKey, List<AccumulatorWrapper>> groups = new HashMap<>();

        /** */
        private Grouping(byte groupId, ImmutableBitSet groupFields) {
            this.groupId = groupId;
            this.groupFields = groupFields;
        }

        /** */
        private void add(T row) {
            if (type == AggregateType.REDUCE)
                addOnReducer(row);
            else
                addOnMapper(row);
        }

        /**
         * @param downstream Target downstream.
         * @param cnt Number of rows to send.
         *
         * @return Actually sent rows number.
         */
        private int send(Downstream<T> downstream, int cnt) {
            if (F.isEmpty(groups))
                return 0;
            else if (type == AggregateType.MAP)
                return sendOnMapper(downstream, cnt);
            else
                return sendOnReducer(downstream, cnt);
        }

        /** */
        private void addOnMapper(T row) {
            GroupKey.Builder b = GroupKey.builder(groupFields.cardinality());

            for (Integer field : groupFields)
                b.add(handler.get(field, row));

            GroupKey groupKey = b.build();

            List<AccumulatorWrapper> wrappers = groups.computeIfAbsent(groupKey, this::create);

            for (AccumulatorWrapper wrapper : wrappers)
                wrapper.add(row);
        }

        /** */
        private void addOnReducer(T row) {
            byte targetGroupId = handler.get(0, row);

            if (targetGroupId != groupId)
                return;

            GroupKey groupKey = handler.get(1, row);

            List<AccumulatorWrapper> wrappers = groups.computeIfAbsent(groupKey, this::create);
            List<Accumulator> accums = handler.get(2, row);

            for (int i = 0; i < wrappers.size(); i++) {
                AccumulatorWrapper wrapper = wrappers.get(i);
                Accumulator accum = accums.get(i);

                wrapper.apply(accum);
            }
        }

        /** */
        private int sendOnMapper(Downstream<T> downstream, int cnt) {
            Iterator<Map.Entry<GroupKey, List<AccumulatorWrapper>>> it = groups.entrySet().iterator();

            int processed = 0;

            while (it.hasNext() && processed < cnt) {
                Map.Entry<GroupKey, List<AccumulatorWrapper>> entry = it.next();

                GroupKey groupKey = entry.getKey();
                List<Accumulator> accums = Commons.transform(entry.getValue(), AccumulatorWrapper::accumulator);

                T row = handler.create(groupId, groupKey, accums);

                downstream.push(row);

                processed++;
                it.remove();
            }

            return processed;
        }

        /** */
        private int sendOnReducer(Downstream<T> downstream, int cnt) {
            Iterator<Map.Entry<GroupKey, List<AccumulatorWrapper>>> it = groups.entrySet().iterator();

            int processed = 0;

            while (it.hasNext() && processed < cnt) {
                Map.Entry<GroupKey, List<AccumulatorWrapper>> entry = it.next();

                GroupKey groupKey = entry.getKey();
                List<AccumulatorWrapper> wrappers = entry.getValue();

                Object[] fields = new Object[groupSet.cardinality() + wrappers.size()];

                int i = 0, j = 0;

                for (Integer field : groupSet)
                    fields[i++] = groupFields.get(field) ? groupKey.field(j++) : null;

                for (AccumulatorWrapper wrapper : wrappers)
                    fields[i++] = wrapper.end();

                T row = handler.create(fields);

                downstream.push(row);

                processed++;
                it.remove();
            }

            return processed;
        }

        /** */
        private List<AccumulatorWrapper> create(GroupKey key) {
            return wrappersFactory.get();
        }
    }
}
