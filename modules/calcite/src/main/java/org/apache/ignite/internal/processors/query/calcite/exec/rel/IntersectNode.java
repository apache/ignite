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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;

/**
 * Execution node for INTERSECT operator.
 */
public class IntersectNode<RowT> extends AbstractSetOpNode<RowT> {
    /**
     *
     */
    public IntersectNode(ExecutionContext<RowT> ctx, RelDataType rowType, AggregateType type, boolean all,
            RowFactory<RowT> rowFactory, int inputsCnt) {
        super(ctx, rowType, type, all, rowFactory, new IntersectGrouping<>(ctx, rowFactory, type, all, inputsCnt));
    }

    /**
     *
     */
    private static class IntersectGrouping<RowT> extends Grouping<RowT> {
        /** Inputs count. */
        private final int inputsCnt;

        /**
         *
         */
        private IntersectGrouping(ExecutionContext<RowT> ctx, RowFactory<RowT> rowFactory, AggregateType type,
                boolean all, int inputsCnt) {
            super(ctx, rowFactory, type, all);

            this.inputsCnt = inputsCnt;
        }

        /** {@inheritDoc} */
        @Override
        protected void endOfSet(int setIdx) {
            if (type == AggregateType.SINGLE && rowsCnt == 0) {
                groups.clear();
            }

            super.endOfSet(setIdx);
        }

        /** {@inheritDoc} */
        @Override
        protected void addOnSingle(RowT row, int setIdx) {
            int[] cntrs;

            GroupKey key = key(row);

            if (setIdx == 0) {
                cntrs = groups.computeIfAbsent(key, k -> new int[inputsCnt]);

                cntrs[0]++;
            } else {
                cntrs = groups.get(key);

                if (cntrs != null) {
                    if (cntrs[setIdx - 1] == 0) {
                        groups.remove(key);
                    } else {
                        cntrs[setIdx]++;
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        protected void addOnMapper(RowT row, int setIdx) {
            int[] cntrs = groups.computeIfAbsent(key(row), k -> new int[inputsCnt]);

            cntrs[setIdx]++;
        }

        /** {@inheritDoc} */
        @Override
        protected boolean affectResult(int[] cntrs) {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        protected int availableRows(int[] cntrs) {
            int cnt = cntrs[0];

            for (int i = 1; i < cntrs.length; i++) {
                if (cntrs[i] < cnt) {
                    cnt = cntrs[i];
                }
            }

            if (all) {
                cntrs[0] = cnt; // Whith this we can decrement only the first element to get the same result.

                return cnt;
            } else {
                return cnt == 0 ? 0 : 1;
            }
        }

        /** {@inheritDoc} */
        @Override
        protected void decrementAvailableRows(int[] cntrs, int amount) {
            assert amount > 0;
            assert all;

            cntrs[0] -= amount;
        }
    }
}
