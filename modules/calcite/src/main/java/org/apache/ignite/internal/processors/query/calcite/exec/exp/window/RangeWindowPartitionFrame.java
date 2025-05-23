package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** {@link WindowFunctionFrame} for RANGE clause. */
final class RangeWindowPartitionFrame<Row> extends WindowFunctionFrame<Row> {

    /**  */
    private final Comparator<Row> peerCmp;

    /**  */
    private final Function<Row, Row> lowerBound;

    /**  */
    private final boolean cacheableLowerBound;

    /**  */
    private final Function<Row, Row> upperBound;

    /**  */
    private final boolean cacheableUpperBound;

    /**  */
    private int peerCount = -1;

    // cache for the row range start.
    private int cachedStartPeerIdx = -1;
    private int cachedStartIdx;

    // cache for the row range end.
    private int cachedEndPeerIdx = -1;
    private int cachedEndIdx;

    /**  */
    RangeWindowPartitionFrame(
        List<Row> buffer,
        ExecutionContext<Row> ctx,
        Comparator<Row> peerCmp,
        Window.Group group,
        RelDataType inputRowType
    ) {
        super(buffer);
        this.peerCmp = peerCmp;
        lowerBound = rangeBoundToProject(ctx, group.lowerBound, group.collation(), inputRowType);
        cacheableLowerBound = isCacheableBound(group.lowerBound);
        upperBound = rangeBoundToProject(ctx, group.upperBound, group.collation(), inputRowType);
        cacheableUpperBound = isCacheableBound(group.upperBound);
    }

    /** {@inheritDoc */
    @Override protected int getFrameStart(Row row, int rowIdx, int peerIdx) {
        if (cacheableLowerBound && cachedStartPeerIdx == peerIdx)
            return cachedStartIdx;

        cachedStartPeerIdx = peerIdx;

        Row lowerBoundRow = lowerBound.apply(row);
        if (lowerBoundRow == null)
            cachedStartIdx = 0;
        else
            cachedStartIdx = bsearchLowerBound(lowerBoundRow, buffer);

        return cachedStartIdx;
    }

    /** {@inheritDoc */
    @Override protected int getFrameEnd(Row row, int rowIdx, int peerIdx) {
        if (cacheableUpperBound && cachedEndPeerIdx == peerIdx)
            return cachedEndIdx;

        cachedEndPeerIdx = peerIdx;

        Row upperBoundRow = upperBound.apply(row);
        if (upperBoundRow == null)
            cachedEndIdx = buffer.size() - 1;
        else
            cachedEndIdx = bsearchUpperBound(upperBoundRow, buffer);

        return cachedEndIdx;
    }

    /** {@inheritDoc */
    @Override int countPeers() {
        if (peerCount == -1) {
            int size = buffer.size();
            if (size == 0)
                peerCount = 0;
            else {
                peerCount = 1;
                Row prevRow = buffer.get(0);
                for (int i = 1; i < size; i++) {
                    Row currRow = buffer.get(i);
                    if (compareRowPeer(prevRow, currRow) != 0) {
                        peerCount++;
                    }
                    prevRow = currRow;
                }
            }
        }

        return peerCount;
    }

    /** {@inheritDoc */
    @Override public void reset() {
        // Reseting index cache.
        cachedStartPeerIdx = -1;
        cachedEndPeerIdx = -1;
        peerCount = -1;
    }

    /** Binary search of lower bound */
    private int bsearchLowerBound(Row row, List<Row> buffer) {
        int start = 0;
        int end = buffer.size() - 1;

        while (start <= end) {
            int mid = (start + end) / 2;

            Row midRow = buffer.get(mid);
            int cmp = compareRowPeer(midRow, row);

            if (cmp == 0) {
                end = mid - 1;
            }
            else if (cmp > 0)
                end = mid - 1;
            else
                start = mid + 1;
        }

        return start;
    }

    /** Binary search of upper bound */
    private int bsearchUpperBound(Row row, List<Row> buffer) {
        int start = 0;
        int end = buffer.size() - 1;

        while (start <= end) {
            int mid = (start + end) / 2;

            Row midRow = buffer.get(mid);
            int cmp = compareRowPeer(midRow, row);

            if (cmp == 0) {
                start = mid + 1;
            }
            else if (cmp > 0)
                end = mid - 1;
            else
                start = mid + 1;
        }

        return end;
    }

    private int compareRowPeer(Row row1, Row row2) {
        // in case peerCmp is not set - all rows has one peer
        return peerCmp == null ? 0 : peerCmp.compare(row1, row2);
    }

    /** Creatre projection for range frame bound */
    private static <Row> Function<Row, Row> rangeBoundToProject(
        ExecutionContext<Row> ctx,
        RexWindowBound bound,
        RelCollation collation,
        RelDataType rowType
    ) {
        if (bound.isCurrentRow())
            return Function.identity();
        else if (bound.isUnbounded())
            return row -> null;
        else {
            assert bound.getOffset() != null : "Unexpected null offset in bounded window";
            assert bound.isPreceding() || bound.isFollowing() : "Unexpected preceding/following in bounded window";
            assert collation.getFieldCollations().size() == 1 : "Unexpected number of field collations in bounded window";

            RelFieldCollation field = collation.getFieldCollations().get(0);
            int fieldIdx = field.getFieldIndex();
            List<RelDataTypeField> fields = rowType.getFieldList();

            IgniteTypeFactory typeFactory = Commons.typeFactory();
            RexBuilder builder = new IgniteRexBuilder(typeFactory);

            List<RexNode> project = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                project.add(builder.makeInputRef(fields.get(i).getType(), i));
            }

            RexNode offset = bound.getOffset();
            if ((bound.isPreceding() && !field.direction.isDescending())
                || (bound.isFollowing() && field.direction.isDescending())) {
                // should invert offset.
                offset = builder.makeCall(SqlStdOperatorTable.UNARY_MINUS, ImmutableList.of(offset));
            }

            SqlOperator operator = SqlStdOperatorTable.PLUS;
            RelDataType fieldType = fields.get(fieldIdx).getType();
            if (SqlTypeFamily.DATETIME.contains(fieldType)
                && SqlTypeFamily.DATETIME_INTERVAL.contains(offset.getType())) {
                operator = SqlStdOperatorTable.DATETIME_PLUS;
            }

            RexNode call = builder.makeCall(operator, ImmutableList.of(project.get(fieldIdx), offset));
            call = builder.makeCast(fieldType, call);

            project.set(fieldIdx, call);

            return ctx.expressionFactory().project(project, rowType);
        }
    }

    private static boolean isCacheableBound(RexWindowBound bound) {
        return bound.isCurrentRow() || bound.isUnbounded() || bound.getOffset() instanceof RexLiteral;
    }
}
