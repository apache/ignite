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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Execution node for MINUS (EXCEPT) operator.
 */
public class MinusNode<Row> extends AbstractNode<Row> {
    /** */
    private final AggregateType type;

    /** */
    private final boolean all;

    /** */
    private final RowFactory<Row> rowFactory;

    /** */
    private final Grouping grouping;

    /** */
    private int requested;

    /** */
    private int waiting;

    /** Current source index. */
    private int curSrcIdx;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     */
    public MinusNode(ExecutionContext<Row> ctx, RelDataType rowType, AggregateType type, boolean all,
        RowFactory<Row> rowFactory) {
        super(ctx, rowType);

        this.all = all;
        this.type = type;
        this.rowFactory = rowFactory;

        grouping = new Grouping();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources());
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0)
            sources().get(curSrcIdx).request(waiting = IN_BUFFER_SIZE);
        else if (!inLoop)
            context().execute(this::flush, this::onError);
    }

    /** */
    public void push(Row row, int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        grouping.add(row, idx);

        if (waiting == 0)
            sources().get(curSrcIdx).request(waiting = IN_BUFFER_SIZE);
    }

    /** */
    public void end(int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert curSrcIdx == idx;

        checkState();

        if (type == AggregateType.SINGLE && grouping.isEmpty())
            curSrcIdx = sources().size(); // Skip subsequent sources.
        else
            curSrcIdx++;

        if (curSrcIdx >= sources().size()) {
            waiting = -1;

            flush();
        }
        else
            sources().get(curSrcIdx).request(waiting);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        grouping.groups.clear();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        return new Downstream<Row>() {
            @Override public void push(Row row) throws Exception {
                MinusNode.this.push(row, idx);
            }

            @Override public void end() throws Exception {
                MinusNode.this.end(idx);
            }

            @Override public void onError(Throwable e) {
                MinusNode.this.onError(e);
            }
        };
    }

    /** */
    private void flush() throws Exception {
        if (isClosed())
            return;

        checkState();

        assert waiting == -1;

        int processed = 0;

        inLoop = true;

        try {
            while (requested > 0 && !grouping.isEmpty()) {
                int toSnd = Math.min(requested, IN_BUFFER_SIZE - processed);

                for (Row row : grouping.getRows(toSnd)) {
                    checkState();

                    requested--;
                    downstream().push(row);

                    processed++;
                }

                if (processed >= IN_BUFFER_SIZE && requested > 0) {
                    // Allow others to do their job.
                    context().execute(this::flush, this::onError);

                    return;
                }
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
    private class Grouping {
        /**
         * Value in this map will always have 2 elements, first - count of keys in the first set, second - count of
         * keys in all sets except first.
         */
        private final Map<GroupKey, int[]> groups = new HashMap<>();

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private Grouping() {
            hnd = context().rowHandler();
        }

        /** */
        private void add(Row row, int setIdx) {
            if (type == AggregateType.REDUCE) {
                assert setIdx == 0 : "Unexpected set index: " + setIdx;

                addOnReducer(row);
            }
            else if (type == AggregateType.MAP)
                addOnMapper(row, setIdx);
            else
                addOnSingle(row, setIdx);
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
                return getOnSingleOrReducer(cnt);
        }

        /** */
        private GroupKey key(Row row) {
            int size = hnd.columnCount(row);

            Object[] fields = new Object[size];

            for (int i = 0; i < size; i++)
                fields[i] = hnd.get(i, row);

            return new GroupKey(fields);
        }

        /** */
        private void addOnSingle(Row row, int setIdx) {
            int[] cntrs;

            GroupKey key = key(row);

            if (setIdx == 0) {
                cntrs = groups.computeIfAbsent(key, k -> new int[2]);

                cntrs[0]++;
            }
            else {
                cntrs = groups.get(key);

                if (cntrs != null) {
                    cntrs[1]++;

                    if (cntrs[1] >= cntrs[0])
                        groups.remove(key);
                }
            }
        }

        /** */
        private void addOnMapper(Row row, int setIdx) {
            int[] cntrs = groups.computeIfAbsent(key(row), k -> new int[2]);

            cntrs[setIdx == 0 ? 0 : 1]++;
        }

        /** */
        private void addOnReducer(Row row) {
            GroupKey grpKey = (GroupKey)hnd.get(0, row);

            int[] cntrs = groups.computeIfAbsent(grpKey, k -> new int[2]);

            int[] cntrsMap = (int[])hnd.get(1, row);

            for (int i = 0; i < cntrsMap.length; i++)
                cntrs[i] += cntrsMap[i];
        }

        /** */
        private List<Row> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<Row> res = new ArrayList<>(amount);

            while (amount > 0 && it.hasNext()) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                // Skip row if it doesn't affect the final result.
                if (entry.getValue()[0] != entry.getValue()[1]) {
                    res.add(rowFactory.create(entry.getKey(), entry.getValue()));

                    amount--;
                }

                it.remove();
            }

            return res;
        }

        /** */
        private List<Row> getOnSingleOrReducer(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            List<Row> res = new ArrayList<>(cnt);

            while (it.hasNext() && cnt > 0) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                GroupKey key = entry.getKey();

                Object[] fields = new Object[key.fieldsCount()];

                for (int i = 0; i < fields.length; i++)
                    fields[i] = key.field(i);

                Row row = rowFactory.create(fields);

                int[] cntrs = entry.getValue();

                int availableRows = availableRows(entry.getValue());

                if (availableRows <= cnt) {
                    it.remove();

                    if (availableRows == 0)
                        continue;

                    cnt -= availableRows;
                }
                else {
                    availableRows = cnt;

                    decrementAvailableRows(cntrs, availableRows);

                    cnt = 0;
                }

                for (int i = 0; i < availableRows; i++)
                    res.add(row);
            }

            return res;
        }

        /** */
        private int availableRows(int[] cntrs) {
            assert cntrs.length == 2;

            if (all)
                return Math.max(cntrs[0] - cntrs[1], 0);
            else
                return cntrs[1] == 0 ? 1 : 0;
        }

        /** */
        private void decrementAvailableRows(int[] cntrs, int amount) {
            assert amount > 0;
            assert all;

            cntrs[0] -= amount;
        }

        /** */
        private boolean isEmpty() {
            return groups.isEmpty();
        }
    }
}
