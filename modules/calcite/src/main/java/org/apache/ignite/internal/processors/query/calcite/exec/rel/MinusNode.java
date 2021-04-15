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
        RowFactory<Row> rowFactory, int sourcesCnt) {
        super(ctx, rowType);

        this.all = all;
        this.type = type;
        this.rowFactory = rowFactory;

        grouping = new Grouping(sourcesCnt);
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
            context().execute(this::doFlush, this::onError);
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
    private void doFlush() throws Exception {
        checkState();

        flush();
    }

    /** */
    private void flush() throws Exception {
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
                    context().execute(this::doFlush, this::onError);

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
        /** */
        private final Map<GroupKey, int[]> groups = new HashMap<>();

        /** */
        private final RowHandler<Row> hnd;

        /** Count of sets (input sources). */
        private final int setsCnt;

        /** */
        private Grouping(int setsCnt) {
            this.setsCnt = setsCnt;
            hnd = context().rowHandler();
        }

        /** */
        private void add(Row row, int setIdx) {
            if (type == AggregateType.REDUCE) {
                assert setIdx == 0 : "Unexpected set index: " + setIdx;

                addOnReducer(row);
            }
            else
                addOnMapper(row, setIdx);
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
        private void addOnMapper(Row row, int setIdx) {
            int[] cntrs = groups.computeIfAbsent(new GroupKey(hnd.getColumns(row)), k -> new int[setsCnt]);

            cntrs[setIdx]++;
        }

        /** */
        private void addOnReducer(Row row) {
            GroupKey grpKey = (GroupKey)hnd.get(0, row);

            int[] cntrs = groups.computeIfAbsent(grpKey, k -> new int[setsCnt]);

            int[] cntrsMap = (int[])hnd.get(1, row);

            for (int i = 0; i < cntrsMap.length; i++)
                cntrs[i] += cntrsMap[i];
        }

        /** */
        private List<Row> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<Row> res = new ArrayList<>(amount);

            for (int i = 0; i < amount; i++) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                res.add(rowFactory.create(entry.getKey(), entry.getValue()));

                it.remove();
            }

            return res;
        }

        /** */
        private List<Row> getOnReducer(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            List<Row> res = new ArrayList<>(cnt);

            while (it.hasNext() && cnt > 0) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                Row row = rowFactory.create(entry.getKey().fields());
                int[] cntrs = entry.getValue();

                int availableRows = availableRows(entry.getValue());

                if (availableRows <= cnt){
                    it.remove();

                    if (availableRows == 0)
                        continue;

                    cnt -= availableRows;
                }
                else {
                    availableRows -= cnt;

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
            int cnt = 0;

            for (int i = 1; i < cntrs.length; i++)
                cnt -= cntrs[i];

            if (all) {
                cnt += cntrs[0];

                return Math.max(cnt, 0);
            }
            else
                return cnt == 0 ? 1 : 0;
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
