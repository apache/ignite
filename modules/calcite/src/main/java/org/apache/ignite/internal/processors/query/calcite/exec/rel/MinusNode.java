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
 * Execution node for MINUS (EXCEPT) operator.
 */
public class MinusNode<Row> extends AbstractSetOpNode<Row> {
    /** */
    public MinusNode(ExecutionContext<Row> ctx, RelDataType rowType, AggregateType type, boolean all,
        RowFactory<Row> rowFactory) {
        super(ctx, rowType, type, all, rowFactory, new MinusGrouping<>(ctx, rowFactory, type, all));
    }

    /** */
    private static class MinusGrouping<Row> extends Grouping<Row> {
        /** */
        private MinusGrouping(ExecutionContext<Row> ctx, RowFactory<Row> rowFactory, AggregateType type, boolean all) {
            super(ctx, rowFactory, type, all);
        }

        /** {@inheritDoc} */
        @Override protected void addOnSingle(Row row, int setIdx) {
            int[] cntrs;

            GroupKey<Row> key = key(row);

            if (setIdx == 0) {
                // Value in the map will always have 2 elements, first - count of keys in the first set,
                // second - count of keys in all sets except first.
                cntrs = groups.computeIfAbsent(key, k -> new int[2]);

                cntrs[0]++;
            }
            else if (all) {
                cntrs = groups.get(key);

                if (cntrs != null) {
                    cntrs[1]++;

                    if (cntrs[1] >= cntrs[0])
                        groups.remove(key);
                }
            }
            else
                groups.remove(key);
        }

        /** {@inheritDoc} */
        @Override protected void addOnMapper(Row row, int setIdx) {
            // Value in the map will always have 2 elements, first - count of keys in the first set,
            // second - count of keys in all sets except first.
            int[] cntrs = groups.computeIfAbsent(key(row), k -> new int[2]);

            cntrs[setIdx == 0 ? 0 : 1]++;
        }

        /** {@inheritDoc} */
        @Override protected boolean affectResult(int[] cntrs) {
            return cntrs[0] != cntrs[1];
        }

        /** {@inheritDoc} */
        @Override protected int availableRows(int[] cntrs) {
            assert cntrs.length == 2;

            if (all)
                return Math.max(cntrs[0] - cntrs[1], 0);
            else
                return cntrs[1] == 0 ? 1 : 0;
        }

        /** {@inheritDoc} */
        @Override protected void decrementAvailableRows(int[] cntrs, int amount) {
            assert amount > 0;
            assert all;

            cntrs[0] -= amount;
        }

        /** {@inheritDoc} */
        @Override protected int countersSize() {
            return 2;
        }
    }
}
