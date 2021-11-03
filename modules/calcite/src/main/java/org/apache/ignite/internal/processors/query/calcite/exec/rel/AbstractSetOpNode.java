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

/**
 * Abstract execution node for set operators (EXCEPT, INTERSECT).
 */
public abstract class AbstractSetOpNode<RowT> extends AbstractNode<RowT> {
    /**
     *
     */
    private final AggregateType type;

    /**
     *
     */
    private final Grouping<RowT> grouping;

    /**
     *
     */
    private int requested;

    /**
     *
     */
    private int waiting;

    /** Current source index. */
    private int curSrcIdx;

    /**
     *
     */
    private boolean inLoop;

    /**
     *
     */
    protected AbstractSetOpNode(ExecutionContext<RowT> ctx, RelDataType rowType, AggregateType type, boolean all,
            RowFactory<RowT> rowFactory, Grouping<RowT> grouping) {
        super(ctx, rowType);

        this.type = type;
        this.grouping = grouping;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources());
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0) {
            sources().get(curSrcIdx).request(waiting = inBufSize);
        } else if (!inLoop) {
            context().execute(this::flush, this::onError);
        }
    }

    /**
     *
     */
    public void push(RowT row, int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        grouping.add(row, idx);

        if (waiting == 0) {
            sources().get(curSrcIdx).request(waiting = inBufSize);
        }
    }

    /**
     *
     */
    public void end(int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert curSrcIdx == idx;

        checkState();

        grouping.endOfSet(idx);

        if (type == AggregateType.SINGLE && grouping.isEmpty()) {
            curSrcIdx = sources().size(); // Skip subsequent sources.
        } else {
            curSrcIdx++;
        }

        if (curSrcIdx >= sources().size()) {
            waiting = -1;

            flush();
        } else {
            sources().get(curSrcIdx).request(waiting);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        grouping.groups.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        return new Downstream<RowT>() {
            @Override
            public void push(RowT row) throws Exception {
                AbstractSetOpNode.this.push(row, idx);
            }

            @Override
            public void end() throws Exception {
                AbstractSetOpNode.this.end(idx);
            }

            @Override
            public void onError(Throwable e) {
                AbstractSetOpNode.this.onError(e);
            }
        };
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

        inLoop = true;

        try {
            if (requested > 0 && !grouping.isEmpty()) {
                int toSnd = Math.min(requested, inBufSize - processed);

                for (RowT row : grouping.getRows(toSnd)) {
                    requested--;

                    downstream().push(row);

                    processed++;
                }

                if (processed >= inBufSize && requested > 0) {
                    // Allow others to do their job.
                    context().execute(this::flush, this::onError);

                    return;
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
    protected abstract static class Grouping<RowT> {
        /**
         *
         */
        protected final Map<GroupKey, int[]> groups = new HashMap<>();

        /**
         *
         */
        protected final RowHandler<RowT> hnd;

        /**
         *
         */
        protected final AggregateType type;

        /**
         *
         */
        protected final boolean all;

        /**
         *
         */
        protected final RowFactory<RowT> rowFactory;

        /** Processed rows count in current set. */
        protected int rowsCnt = 0;

        /**
         *
         */
        protected Grouping(ExecutionContext<RowT> ctx, RowFactory<RowT> rowFactory, AggregateType type, boolean all) {
            hnd = ctx.rowHandler();
            this.type = type;
            this.all = all;
            this.rowFactory = rowFactory;
        }

        /**
         *
         */
        private void add(RowT row, int setIdx) {
            if (type == AggregateType.REDUCE) {
                assert setIdx == 0 : "Unexpected set index: " + setIdx;

                addOnReducer(row);
            } else if (type == AggregateType.MAP) {
                addOnMapper(row, setIdx);
            } else {
                addOnSingle(row, setIdx);
            }

            rowsCnt++;
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
                return getOnSingleOrReducer(cnt);
            }
        }

        /**
         *
         */
        protected GroupKey key(RowT row) {
            int size = hnd.columnCount(row);

            Object[] fields = new Object[size];

            for (int i = 0; i < size; i++) {
                fields[i] = hnd.get(i, row);
            }

            return new GroupKey(fields);
        }

        /**
         *
         */
        protected void endOfSet(int setIdx) {
            rowsCnt = 0;
        }

        /**
         *
         */
        protected abstract void addOnSingle(RowT row, int setIdx);

        /**
         *
         */
        protected abstract void addOnMapper(RowT row, int setIdx);

        /**
         *
         */
        protected void addOnReducer(RowT row) {
            GroupKey grpKey = (GroupKey) hnd.get(0, row);
            int[] cntrsMap = (int[]) hnd.get(1, row);

            int[] cntrs = groups.computeIfAbsent(grpKey, k -> new int[cntrsMap.length]);

            assert cntrs.length == cntrsMap.length;

            for (int i = 0; i < cntrsMap.length; i++) {
                cntrs[i] += cntrsMap[i];
            }
        }

        /**
         *
         */
        protected List<RowT> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<RowT> res = new ArrayList<>(amount);

            while (amount > 0 && it.hasNext()) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                // Skip row if it doesn't affect the final result.
                if (affectResult(entry.getValue())) {
                    res.add(rowFactory.create(entry.getKey(), entry.getValue()));

                    amount--;
                }

                it.remove();
            }

            return res;
        }

        /**
         *
         */
        protected List<RowT> getOnSingleOrReducer(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            List<RowT> res = new ArrayList<>(cnt);

            while (it.hasNext() && cnt > 0) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                GroupKey key = entry.getKey();

                Object[] fields = new Object[key.fieldsCount()];

                for (int i = 0; i < fields.length; i++) {
                    fields[i] = key.field(i);
                }

                RowT row = rowFactory.create(fields);

                int[] cntrs = entry.getValue();

                int availableRows = availableRows(entry.getValue());

                if (availableRows <= cnt) {
                    it.remove();

                    if (availableRows == 0) {
                        continue;
                    }

                    cnt -= availableRows;
                } else {
                    availableRows = cnt;

                    decrementAvailableRows(cntrs, availableRows);

                    cnt = 0;
                }

                for (int i = 0; i < availableRows; i++) {
                    res.add(row);
                }
            }

            return res;
        }

        /**
         * Return {@code true} if counters affects the final result, or {@code false} if row can be skipped.
         */
        protected abstract boolean affectResult(int[] cntrs);

        /**
         *
         */
        protected abstract int availableRows(int[] cntrs);

        /**
         *
         */
        protected abstract void decrementAvailableRows(int[] cntrs, int amount);

        /**
         *
         */
        private boolean isEmpty() {
            return groups.isEmpty();
        }
    }
}
