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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/** */
public final class WindowFunctions {

    /** Check window group can be processed with streaming partition. */
    public static boolean streamable(Window.Group group) {
        // Can execute window streaming in case:
        // - group aggs does not contain operators can access whole partition.
        if (group.aggCalls.stream().anyMatch(it -> BUFFERING_FUNCTIONS.contains(it.op)))
            return false;

        // - group aggs contains only ROW_NUMBER, RANK, DENSE_RANK operators
        if (group.aggCalls.stream().allMatch(it -> STREAMING_FUNCTIONS.contains(it.op)))
            return true;

        // group frame in 'ROWS BETWEEN UNBOUNDED PRESCENDING AND CURRENT ROW'
        //noinspection RedundantIfStatement
        if (group.isRows && group.lowerBound.isUnbounded() && group.upperBound.isCurrentRow())
            return true;

        return false;
    }

    /** Determines if the specified {@link SqlOperator} supports sreaming execution. */
    static boolean isStreamingFunction(SqlOperator op) {
        return STREAMING_FUNCTIONS.contains(op);
    }

    /** Window functions, which definitly supports sreaming execution. */
    private static final Set<SqlOperator> STREAMING_FUNCTIONS = Set.of(
        SqlStdOperatorTable.ROW_NUMBER,
        SqlStdOperatorTable.RANK,
        SqlStdOperatorTable.DENSE_RANK
    );

    /** Window functions, which definitly requires buffering. */
    private static final Set<SqlOperator> BUFFERING_FUNCTIONS = Set.of(
        SqlStdOperatorTable.PERCENT_RANK,
        SqlStdOperatorTable.CUME_DIST,
        SqlStdOperatorTable.FIRST_VALUE,
        SqlStdOperatorTable.LAST_VALUE,
        SqlStdOperatorTable.LAG,
        SqlStdOperatorTable.LEAD,
        SqlStdOperatorTable.NTILE,
        SqlStdOperatorTable.NTH_VALUE
    );

    /** Determines if the specified SqlOperator is a window function call. */
    static boolean isWindowFunction(AggregateCall call) {
        return STREAMING_FUNCTIONS.contains(call.getAggregation())
            || BUFFERING_FUNCTIONS.contains(call.getAggregation());
    }

    /** */
    static <Row> Supplier<WindowFunction<Row>> windowFunctionFactory(
        AggregateCall call,
        ExecutionContext<Row> ctx
    ) {
        RowHandler<Row> hnd = ctx.rowHandler();
        switch (call.getAggregation().getName()) {
            case "ROW_NUMBER":
                return () -> new RowNumber<>(hnd, call);
            case "RANK":
                return () -> new Rank<>(hnd, call);
            case "DENSE_RANK":
                return () -> new DenseRank<>(hnd, call);
            case "PERCENT_RANK":
                return () -> new PercentRank<>(hnd, call);
            case "CUME_DIST":
                return () -> new CumeDist<>(hnd, call);
            case "LAG":
                return () -> new Lag<>(hnd, call);
            case "LEAD":
                return () -> new Lead<>(hnd, call);
            case "FIRST_VALUE":
                return () -> new FirstValue<>(hnd, call);
            case "LAST_VALUE":
                return () -> new LastValue<>(hnd, call);
            case "NTILE":
                return () -> new NTile<>(hnd, call);
            case "NTH_VALUE":
                return () -> new NthValue<>(hnd, call);
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    /** */
    private abstract static class AbstractWindowFunction<Row> {

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private final AggregateCall aggCall;

        /** */
        private AbstractWindowFunction(RowHandler<Row> hnd, AggregateCall aggCall) {
            this.hnd = hnd;
            this.aggCall = aggCall;
        }

        /** */
        <T> T get(int idx, Row row) {
            assert idx < arguments().size() : "idx=" + idx + "; arguments=" + arguments();

            return (T)hnd.get(arguments().get(idx), row);
        }

        /** */
        protected AggregateCall aggregateCall() {
            return aggCall;
        }

        /** */
        protected List<Integer> arguments() {
            return aggCall.getArgList();
        }

        /** */
        int columnCount(Row row) {
            return hnd.columnCount(row);
        }
    }

    /** */
    private abstract static class AbstractLagLeadWindowFunction<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {
        /** */
        private AbstractLagLeadWindowFunction(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** */
        protected int getOffset(Row row) {
            if (arguments().size() > 1)
                return get(1, row);
            else
                return 1;
        }

        /** */
        protected Object getDefault(Row row) {
            if (arguments().size() > 2)
                return get(2, row);
            else
                return null;
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int offset = getOffset(row);
            int idx = applyOffset(rowIdx, offset);
            if (idx < 0 || idx >= frame.partitionSize())
                return getDefault(row);
            else {
                Row offsetRow = frame.get(idx);
                Object val = get(0, offsetRow);
                if (val == null) {
                    val = getDefault(row);
                }
                return val;
            }
        }

        /** */
        protected abstract int applyOffset(int rowIdx, int offset);

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            int argSize = arguments().size();
            assert argSize >= 1 && argSize <= 3 : "Unexpected arguments count: " + argSize;

            ImmutableList.Builder<RelDataType> builder = ImmutableList.builderWithExpectedSize(argSize);
            builder.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
            if (argSize > 1) {
                builder.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), false));
            }
            if (argSize > 2) {
                builder.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
            }
            return builder.build();
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true);
        }
    }

    /** ROW_NUMBER window function implementation. */
    private static class RowNumber<Row> extends AbstractWindowFunction<Row> implements StreamWindowFunction<Row> {

        /** */
        private RowNumber(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx) {
            return rowIdx + 1;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** RANK window function implementation. */
    private static class Rank<Row> extends AbstractWindowFunction<Row> implements StreamWindowFunction<Row> {
        /** */
        private int previousPeerIdx = -1;

        /** */
        private long rank = 1;

        /** */
        private long cnt;

        /** */
        private Rank(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx) {
            if (previousPeerIdx != peerIdx) {
                previousPeerIdx = peerIdx;
                rank += cnt;
                cnt = 0;
            }
            cnt++;
            return rank;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** DENSE_RANK window function implementation. */
    private static class DenseRank<Row> extends AbstractWindowFunction<Row> implements StreamWindowFunction<Row> {

        /** */
        private DenseRank(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx) {
            return peerIdx + 1;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** PERCENT_RANK window function implementation. */
    private static class PercentRank<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        /** */
        private final Rank<Row> rank;

        /** */
        private PercentRank(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
            rank = new Rank<>(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int size = frame.partitionSize() - 1;
            if (size == 0)
                return 0.0;
            else {
                long rank = (Long)this.rank.call(row, rowIdx, peerIdx, frame);
                return ((double)rank - 1) / size;
            }
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(DOUBLE);
        }
    }

    /** CUME_DIST window function implementation. */
    private static class CumeDist<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        /** */
        private CumeDist(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int cnt = frame.size(rowIdx, peerIdx);
            return ((double)cnt) / frame.partitionSize();
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(DOUBLE);
        }
    }

    /** LAG window function implementation. */
    private static class Lag<Row> extends AbstractLagLeadWindowFunction<Row> {

        /** */
        private Lag(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override protected int applyOffset(int rowIdx, int offset) {
            return rowIdx - offset;
        }
    }

    /** LEAD window function implementation. */
    private static class Lead<Row> extends AbstractLagLeadWindowFunction<Row> {

        /** */
        private Lead(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override protected int applyOffset(int rowIdx, int offset) {
            return rowIdx + offset;
        }
    }

    /** FIRST_VALUE window function implementation. */
    private static class FirstValue<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        /** */
        private FirstValue(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int startIdx = frame.getFrameStart(row, rowIdx, peerIdx);
            Row firstRow = frame.get(startIdx);
            return get(0, firstRow);
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true);
        }
    }

    /** LAST_VALUE window function implementation. */
    private static class LastValue<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {
        /** */
        private LastValue(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int endIdx = frame.getFrameEnd(row, rowIdx, peerIdx);
            if (endIdx < 0)
                return null;
            else {
                Row lastRow = frame.get(endIdx);
                return get(0, lastRow);
            }
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true);
        }
    }

    /** NTILE window function implementation. */
    private static class NTile<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        /** */
        private NTile(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int buckets = get(0, row);
            int rowCnt = frame.partitionSize();
            if (buckets >= rowCnt)
                return rowIdx + 1;
            else {
                int rowsPerBucket = rowCnt / buckets;
                int remainderRows = rowCnt % buckets;

                // Remainder rows are assigned starting from the first bucket.
                // Thus, each of those buckets has an additional row.
                if (rowIdx < ((rowsPerBucket + 1) * remainderRows))
                    return (rowIdx / (rowsPerBucket + 1)) + 1;
                else
                    return ((rowIdx - remainderRows) / rowsPerBucket) + 1;
            }
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createSqlType(INTEGER));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** NTH_VALUE window function implementation. */
    private static class NthValue<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        /** */
        private NthValue(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int offset = get(1, row);
            if (offset < 1) {
                throw new IllegalArgumentException("Offset must be at least 1.");
            }

            int startIdx = frame.getFrameStart(row, rowIdx, peerIdx);
            int endIdx = frame.getFrameEnd(row, rowIdx, peerIdx);
            // offset is base 1
            int valIdx = startIdx + offset - 1;
            if (valIdx > endIdx)
                return null;
            else {
                Row valRow = frame.get(valIdx);
                return get(0, valRow);
            }
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createSqlType(ANY), typeFactory.createSqlType(INTEGER));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(ANY);
        }
    }
}
