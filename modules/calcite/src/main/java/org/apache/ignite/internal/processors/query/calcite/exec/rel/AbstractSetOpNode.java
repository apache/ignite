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
 * Abstract execution node for set operators (EXCEPT, INTERSECT).
 */
public abstract class AbstractSetOpNode<Row> extends MemoryTrackingNode<Row> {
    /** */
    private final AggregateType type;

    /** */
    private final Grouping<Row> grouping;

    /** */
    private int requested;

    /** */
    private int waiting;

    /** Current source index. */
    private int curSrcIdx;

    /** */
    private boolean inLoop;

    /** */
    protected AbstractSetOpNode(ExecutionContext<Row> ctx, RelDataType rowType, AggregateType type, boolean all,
        RowFactory<Row> rowFactory, Grouping<Row> grouping) {
        super(ctx, rowType, HASH_MAP_ROW_OVERHEAD + grouping.countersSize() * Integer.BYTES);

        this.type = type;
        this.grouping = grouping;
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

        int size = grouping.size();

        grouping.add(row, idx);

        if (grouping.size() > size)
            nodeMemoryTracker.onRowAdded(row);
        else if (grouping.size() < size)
            nodeMemoryTracker.onRowRemoved(row);

        if (waiting == 0)
            sources().get(curSrcIdx).request(waiting = IN_BUFFER_SIZE);
    }

    /** */
    public void end(int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert curSrcIdx == idx;

        checkState();

        grouping.endOfSet(idx);

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
        curSrcIdx = 0;
        grouping.groups.clear();
        nodeMemoryTracker.reset();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        return new Downstream<Row>() {
            @Override public void push(Row row) throws Exception {
                AbstractSetOpNode.this.push(row, idx);
            }

            @Override public void end() throws Exception {
                AbstractSetOpNode.this.end(idx);
            }

            @Override public void onError(Throwable e) {
                AbstractSetOpNode.this.onError(e);
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
            if (requested > 0 && !grouping.isEmpty()) {
                int toSnd = Math.min(requested, IN_BUFFER_SIZE - processed);

                int size = grouping.size();

                List<Row> rows = grouping.getRows(toSnd);

                int removed = size - grouping.size();

                for (Row row : rows) {
                    requested--;

                    downstream().push(row);

                    if (processed < removed)
                        nodeMemoryTracker.onRowRemoved(row);

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
    protected abstract static class Grouping<Row> {
        /** */
        protected final Map<GroupKey<Row>, int[]> groups = new HashMap<>();

        /** */
        protected final RowHandler<Row> hnd;

        /** */
        protected final AggregateType type;

        /** */
        protected final boolean all;

        /** */
        protected final RowFactory<Row> rowFactory;

        /** Processed rows count in current set. */
        protected int rowsCnt = 0;

        /** */
        protected Grouping(ExecutionContext<Row> ctx, RowFactory<Row> rowFactory, AggregateType type, boolean all) {
            hnd = ctx.rowHandler();
            this.type = type;
            this.all = all;
            this.rowFactory = rowFactory;
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

            rowsCnt++;
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
        protected GroupKey<Row> key(Row row) {
            return new GroupKey<>(row, hnd);
        }

        /** */
        protected void endOfSet(int setIdx) {
            rowsCnt = 0;
        }

        /** */
        protected abstract void addOnSingle(Row row, int setIdx);

        /** */
        protected abstract void addOnMapper(Row row, int setIdx);

        /** */
        protected void addOnReducer(Row row) {
            GroupKey<Row> grpKey = key((Row)hnd.get(0, row));
            int[] cntrsMap = (int[])hnd.get(1, row);

            int[] cntrs = groups.computeIfAbsent(grpKey, k -> new int[cntrsMap.length]);

            assert cntrs.length == cntrsMap.length;

            for (int i = 0; i < cntrsMap.length; i++)
                cntrs[i] += cntrsMap[i];
        }

        /** */
        protected List<Row> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey<Row>, int[]>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<Row> res = new ArrayList<>(amount);

            while (amount > 0 && it.hasNext()) {
                Map.Entry<GroupKey<Row>, int[]> entry = it.next();

                // Skip row if it doesn't affect the final result.
                if (affectResult(entry.getValue())) {
                    res.add(rowFactory.create(entry.getKey().row(), entry.getValue()));

                    amount--;
                }

                it.remove();
            }

            return res;
        }

        /** */
        protected List<Row> getOnSingleOrReducer(int cnt) {
            Iterator<Map.Entry<GroupKey<Row>, int[]>> it = groups.entrySet().iterator();

            List<Row> res = new ArrayList<>(cnt);

            while (it.hasNext() && cnt > 0) {
                Map.Entry<GroupKey<Row>, int[]> entry = it.next();

                GroupKey<Row> key = entry.getKey();

                Row row = key.row();

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

        /**
         * Return {@code true} if counters affects the final result, or {@code false} if row can be skipped.
         */
        protected abstract boolean affectResult(int[] cntrs);

        /** */
        protected abstract int availableRows(int[] cntrs);

        /** */
        protected abstract void decrementAvailableRows(int[] cntrs, int amount);

        /** */
        private boolean isEmpty() {
            return groups.isEmpty();
        }

        /** */
        private int size() {
            return groups.size();
        }

        /** */
        protected abstract int countersSize();
    }
}
