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

import static java.util.stream.Collectors.toCollection;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.negate;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.lang.IgniteInternalException;

/**
 *
 */
public class HashAggregateNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
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
    private final List<Grouping> groupings;

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
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     */
    public HashAggregateNode(
            ExecutionContext<RowT> ctx, RelDataType rowType, AggregateType type, List<ImmutableBitSet> grpSets,
            Supplier<List<AccumulatorWrapper<RowT>>> accFactory, RowFactory<RowT> rowFactory) {
        super(ctx, rowType);

        this.type = type;
        this.accFactory = accFactory;
        this.rowFactory = rowFactory;

        ImmutableBitSet.Builder b = ImmutableBitSet.builder();

        if (grpSets.size() > Byte.MAX_VALUE) {
            throw new IgniteInternalException("Too many groups");
        }

        groupings = new ArrayList<>(grpSets.size());

        for (byte i = 0; i < grpSets.size(); i++) {
            ImmutableBitSet grpFields = grpSets.get(i);
            groupings.add(new Grouping(i, grpFields));

            b.addAll(grpFields);
        }

        grpSet = b.build();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (!inLoop) {
            context().execute(this::flush, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        for (Grouping grouping : groupings) {
            grouping.add(row);
        }

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        groupings.forEach(grouping -> grouping.groups.clear());
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
    private void flush() throws Exception {
        if (isClosed()) {
            return;
        }

        checkState();

        assert waiting == -1;

        int processed = 0;
        ArrayDeque<Grouping> groupingsQueue = groupingsQueue();

        inLoop = true;
        try {
            while (requested > 0 && !groupingsQueue.isEmpty()) {
                Grouping grouping = groupingsQueue.peek();

                int toSnd = Math.min(requested, inBufSize - processed);

                for (RowT row : grouping.getRows(toSnd)) {
                    checkState();

                    requested--;
                    downstream().push(row);

                    processed++;
                }

                if (processed >= inBufSize && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flush, this::onError);

                    return;
                }

                if (grouping.isEmpty()) {
                    groupingsQueue.remove();
                }
            }
        } finally {
            inLoop = false;
        }

        if (requested > 0) {
            requested = 0;
            downstream().end();
        }
    }

    /**
     *
     */
    private ArrayDeque<Grouping> groupingsQueue() {
        return groupings.stream()
                .filter(negate(Grouping::isEmpty))
                .collect(toCollection(ArrayDeque::new));
    }

    /**
     *
     */
    private class Grouping {
        /**
         *
         */
        private final byte grpId;

        /**
         *
         */
        private final ImmutableBitSet grpFields;

        /**
         *
         */
        private final Map<GroupKey, List<AccumulatorWrapper<RowT>>> groups = new HashMap<>();

        /**
         *
         */
        private final RowHandler<RowT> handler;

        /**
         *
         */
        private Grouping(byte grpId, ImmutableBitSet grpFields) {
            this.grpId = grpId;
            this.grpFields = grpFields;

            handler = context().rowHandler();

            // Initializes aggregates for case when no any rows will be added into the aggregate to have 0 as result.
            // Doesn't do it for MAP type due to we don't want send from MAP node zero results because it looks redundant.
            if (grpFields.isEmpty() && (type == AggregateType.REDUCE || type == AggregateType.SINGLE)) {
                groups.put(GroupKey.EMPTY_GRP_KEY, create(GroupKey.EMPTY_GRP_KEY));
            }
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
         * @param cnt Number of rows.
         * @return Actually sent rows number.
         */
        private List<RowT> getRows(int cnt) {
            if (nullOrEmpty(groups)) {
                return Collections.emptyList();
            } else if (type == AggregateType.MAP) {
                return getOnMapper(cnt);
            } else {
                return getOnReducer(cnt);
            }
        }

        /**
         *
         */
        private void addOnMapper(RowT row) {
            GroupKey.Builder b = GroupKey.builder(grpFields.cardinality());

            for (Integer field : grpFields) {
                b.add(handler.get(field, row));
            }

            GroupKey grpKey = b.build();

            List<AccumulatorWrapper<RowT>> wrappers = groups.computeIfAbsent(grpKey, this::create);

            for (AccumulatorWrapper<RowT> wrapper : wrappers) {
                wrapper.add(row);
            }
        }

        /**
         *
         */
        private void addOnReducer(RowT row) {
            byte targetGrpId = (byte) handler.get(0, row);

            if (targetGrpId != grpId) {
                return;
            }

            GroupKey grpKey = (GroupKey) handler.get(1, row);

            List<AccumulatorWrapper<RowT>> wrappers = groups.computeIfAbsent(grpKey, this::create);
            List<Accumulator> accums = hasAccumulators() ? (List<Accumulator>) handler.get(2, row) : Collections.emptyList();

            for (int i = 0; i < wrappers.size(); i++) {
                AccumulatorWrapper<RowT> wrapper = wrappers.get(i);
                Accumulator accum = accums.get(i);

                wrapper.apply(accum);
            }
        }

        /**
         *
         */
        private List<RowT> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey, List<AccumulatorWrapper<RowT>>>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<RowT> res = new ArrayList<>(amount);

            for (int i = 0; i < amount; i++) {
                Map.Entry<GroupKey, List<AccumulatorWrapper<RowT>>> entry = it.next();

                GroupKey grpKey = entry.getKey();
                List<Accumulator> accums = Commons.transform(entry.getValue(), AccumulatorWrapper::accumulator);

                RowT row = hasAccumulators() ? rowFactory.create(grpId, grpKey, accums) : rowFactory.create(grpId, grpKey);

                res.add(row);

                it.remove();
            }

            return res;
        }

        /**
         *
         */
        private List<RowT> getOnReducer(int cnt) {
            Iterator<Map.Entry<GroupKey, List<AccumulatorWrapper<RowT>>>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<RowT> res = new ArrayList<>(amount);

            for (int i = 0; i < amount; i++) {
                Map.Entry<GroupKey, List<AccumulatorWrapper<RowT>>> entry = it.next();

                GroupKey grpKey = entry.getKey();
                List<AccumulatorWrapper<RowT>> wrappers = entry.getValue();

                Object[] fields = new Object[grpSet.cardinality() + wrappers.size()];

                int j = 0;
                int k = 0;

                for (Integer field : grpSet) {
                    fields[j++] = grpFields.get(field) ? grpKey.field(k++) : null;
                }

                for (AccumulatorWrapper<RowT> wrapper : wrappers) {
                    fields[j++] = wrapper.end();
                }

                res.add(rowFactory.create(fields));
                it.remove();
            }

            return res;
        }

        /**
         *
         */
        private List<AccumulatorWrapper<RowT>> create(GroupKey key) {
            if (accFactory == null) {
                return Collections.emptyList();
            }

            return accFactory.get();
        }

        /**
         *
         */
        private boolean isEmpty() {
            return groups.isEmpty();
        }
    }
}
