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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.util.typedef.F;

import static java.util.stream.Collectors.toCollection;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.negate;

/**
 *
 */
public class HashAggregateNode<Row> extends AggregateNode<Row> {
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
    public HashAggregateNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        AggregateType type,
        List<ImmutableBitSet> grpSets,
        Supplier<List<AccumulatorWrapper<Row>>> accFactory,
        RowFactory<Row> rowFactory
    ) {
        super(ctx, rowType, type, accFactory, rowFactory, rowOverhead(type, grpSets));

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

    /** */
    private static long rowOverhead(AggregateType type, List<ImmutableBitSet> grpSets) {
        if (type == AggregateType.REDUCE) // On reduce node each row affects only one group.
            return HASH_MAP_ROW_OVERHEAD;
        else // Assume half of groups are affected in case row is added to at least one of them.
            return HASH_MAP_ROW_OVERHEAD * grpSets.size() / 2;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
        else if (!inLoop)
            context().execute(this::flush, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        boolean groupingsChanged = false;

        for (Grouping grouping : groupings) {
            int size = groupings.size();

            grouping.add(row);

            if (grouping.size() > size)
                groupingsChanged = true;
        }

        // It's a very rough estimate of memory consumption for this node.
        // To calculate precise size several aspects should be taken into account:
        //  - There are intersections possible between column sets of groupings, in this case size of object refered by
        //    intersected columns must be calculated only once.
        //  - If accumulators contain AggAccumulator, then GroupKey columns will be refered to objects which are refered
        //    by rows in AggAccumulator too.
        //  - Logic for map and reduce nodes should be completly different, we should take into account GroupKey and
        //    accumulators wrapping into a row on reduce node.
        //  - Etc.
        // So, precise size calculation can be complicated and can affect performance.
        // To simplify the calculation, here we assuming that all objects referenced by row are used by groupings or
        // aggregations (all redundant columns are dropped by optimizer earlier), so, just calculating the size of the
        // whole row we have close to real memory consumption by row referenced objects (except service structures).
        // Also we can guess size of service structures required by grouping and use it as constant row overhead.
        if (hasAggAccum || groupingsChanged)
            nodeMemoryTracker.onRowAdded(row);

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        groupings.forEach(Grouping::reset);
        nodeMemoryTracker.reset();
    }

    /** */
    private void flush() throws Exception {
        if (isClosed())
            return;

        checkState();

        assert waiting == -1;

        int processed = 0;
        ArrayDeque<Grouping> groupingsQueue = groupingsQueue();

        inLoop = true;
        try {
            while (requested > 0 && !groupingsQueue.isEmpty()) {
                Grouping grouping = groupingsQueue.peek();

                int toSnd = Math.min(requested, IN_BUFFER_SIZE - processed);

                for (Row row : grouping.getRows(toSnd)) {
                    checkState();

                    requested--;
                    downstream().push(row);

                    processed++;
                }

                if (processed >= IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flush, this::onError);

                    return;
                }

                if (grouping.isEmpty())
                    groupingsQueue.remove();
            }
        }
        finally {
            inLoop = false;
        }

        if (requested > 0) {
            requested = 0;
            downstream().end();
        }
    }

    /** */
    private ArrayDeque<Grouping> groupingsQueue() {
        return groupings.stream()
            .filter(negate(Grouping::isEmpty))
            .collect(toCollection(ArrayDeque::new));
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
        private GroupKey.Builder grpKeyBld;

        /** */
        private final BiFunction<GroupKey, List<AccumulatorWrapper<Row>>, List<AccumulatorWrapper<Row>>> getOrCreateGroup;

        /** */
        private Grouping(byte grpId, ImmutableBitSet grpFields) {
            this.grpId = grpId;
            this.grpFields = grpFields;

            grpKeyBld = GroupKey.builder(grpFields.cardinality());
            handler = context().rowHandler();

            getOrCreateGroup = (k, v) -> {
                if (v == null) {
                    grpKeyBld = GroupKey.builder(grpFields.cardinality());

                    return create();
                }
                else {
                    grpKeyBld.clear();

                    return v;
                }
            };

            init();
        }

        /** */
        private void init() {
            // Initializes aggregates for case when no any rows will be added into the aggregate to have 0 as result.
            // Doesn't do it for MAP type due to we don't want send from MAP node zero results because it looks redundant.
            if (grpFields.isEmpty() && (type == AggregateType.REDUCE || type == AggregateType.SINGLE))
                groups.put(GroupKey.EMPTY_GRP_KEY, create());
        }

        /** */
        private void reset() {
            groups.clear();

            init();
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
            for (Integer field : grpFields)
                grpKeyBld.add(handler.get(field, row));

            List<AccumulatorWrapper<Row>> wrappers = groups.compute(grpKeyBld.build(), getOrCreateGroup);

            for (AccumulatorWrapper<Row> wrapper : wrappers)
                wrapper.add(row);
        }

        /** */
        private void addOnReducer(Row row) {
            byte targetGrpId = (byte)handler.get(0, row);

            if (targetGrpId != grpId)
                return;

            GroupKey grpKey = (GroupKey)handler.get(1, row);

            List<AccumulatorWrapper<Row>> wrappers = groups.computeIfAbsent(grpKey, (k) -> create());
            Accumulator<Row>[] accums = hasAccumulators() ? (Accumulator<Row>[])handler.get(2, row) : null;

            for (int i = 0; i < wrappers.size(); i++) {
                AccumulatorWrapper<Row> wrapper = wrappers.get(i);
                Accumulator<Row> accum = accums[i];

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
                if (hasAccumulators()) {
                    List<AccumulatorWrapper<Row>> wrappers = entry.getValue();
                    Accumulator<Row>[] accums = new Accumulator[wrappers.size()];

                    for (int j = 0; j < wrappers.size(); j++)
                        accums[j] = wrappers.get(j).accumulator();

                    res.add(rowFactory.create(grpId, grpKey, accums));
                }
                else
                    res.add(rowFactory.create(grpId, grpKey));

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

                Object[] keyFields = grpKey.fields();

                for (Integer field : grpSet)
                    fields[j++] = grpFields.get(field) ? keyFields[k++] : null;

                for (AccumulatorWrapper<Row> wrapper : wrappers)
                    fields[j++] = wrapper.end();

                res.add(rowFactory.create(fields));
                it.remove();
            }

            return res;
        }

        /** */
        private List<AccumulatorWrapper<Row>> create() {
            if (accFactory == null)
                return Collections.emptyList();

            return accFactory.get();
        }

        /** */
        private boolean isEmpty() {
            return groups.isEmpty();
        }

        /** */
        private int size() {
            return groups.size();
        }
    }
}
