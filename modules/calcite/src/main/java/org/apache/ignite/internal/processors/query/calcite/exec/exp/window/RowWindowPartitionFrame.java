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
import java.util.function.Function;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/** {@link WindowPartitionFrame} for ROWS clause. */
final class RowWindowPartitionFrame<Row> extends WindowPartitionFrame<Row> {
    /** Returns the offset that marks the start of the frame. */
    private final Function<Row, Integer> lowerBoundOffset;

    /** Returns the offset that marks the end of the frame. */
    private final Function<Row, Integer> upperBoundOffset;

    /** Cached row idx for which the frame start offset has been computed. */
    private int cachedStartRowIdx = -1;

    /** Cached frame start offset. */
    private Integer cachedStartOffset;

    /** Cached row idx for which the frame end offset has been computed. */
    private int cachedEndRowIdx = -1;

    /** Cached frame end offset. */
    private Integer cachedEndOffset;

    /** */
    RowWindowPartitionFrame(
        List<Row> buf,
        ExecutionContext<Row> ctx,
        Window.Group grp,
        RelDataType inputRowType
    ) {
        super(buf);
        lowerBoundOffset = rowsBoundToOffset(ctx, grp.lowerBound, inputRowType);
        upperBoundOffset = rowsBoundToOffset(ctx, grp.upperBound, inputRowType);
    }

    /** {@inheritDoc} */
    @Override int getFrameStart(int rowIdx, int peerIdx) {
        if (cachedStartRowIdx != rowIdx) {
            Row row = get(rowIdx);
            cachedStartRowIdx = rowIdx;
            cachedStartOffset = lowerBoundOffset.apply(row);
        }

        if (cachedStartOffset == null)
            return 0;
        else {
            int idx = applyOffset(rowIdx, cachedStartOffset, size() - 1);
            return Math.max(idx, 0);
        }
    }

    /** {@inheritDoc} */
    @Override int getFrameEnd(int rowIdx, int peerIdx) {
        if (cachedEndRowIdx != rowIdx) {
            Row row = get(rowIdx);
            cachedEndRowIdx = rowIdx;
            cachedEndOffset = upperBoundOffset.apply(row);
        }

        if (cachedEndOffset == null)
            return size() - 1;
        else
            return applyOffset(rowIdx, cachedEndOffset, size() - 1);
    }

    /** */
    private static int applyOffset(int rowIdx, int offset, int cap) {
        int idx = Math.addExact(rowIdx, offset);
        return Math.max(Math.min(idx, cap), -1);
    }

    /** Create projection for range frame bound. */
    private static <Row> Function<Row, Integer> rowsBoundToOffset(
        ExecutionContext<Row> ctx,
        RexWindowBound bound,
        RelDataType rowType
    ) {
        if (bound.isCurrentRow())
            return ignored -> 0;
        else if (bound.isUnbounded())
            return ignored -> null;
        else {
            assert bound.getOffset() != null : "Unexpected null offset in bounded window";

            IgniteTypeFactory typeFactory = Commons.typeFactory();
            RexBuilder builder = new IgniteRexBuilder(typeFactory);
            RexNode result = builder.makeCast(typeFactory.createSqlType(INTEGER), bound.getOffset());
            if (bound.isPreceding())
                result = builder.makeCall(SqlStdOperatorTable.UNARY_MINUS, ImmutableList.of(result));
            Function<Row, Row> project = ctx.expressionFactory().project(List.of(result), rowType);
            return project.andThen(row -> (Integer)ctx.rowHandler().get(0, row));
        }
    }
}
