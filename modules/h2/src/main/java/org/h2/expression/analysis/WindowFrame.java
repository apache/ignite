/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.BinaryOperation;
import org.h2.expression.BinaryOperation.OpType;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Window frame clause.
 */
public final class WindowFrame {

    private static abstract class Itr implements Iterator<Value[]> {

        final ArrayList<Value[]> orderedRows;

        int cursor;

        Itr(ArrayList<Value[]> orderedRows) {
            this.orderedRows = orderedRows;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private static class PlainItr extends Itr {

        final int endIndex;

        PlainItr(ArrayList<Value[]> orderedRows, int startIndex, int endIndex) {
            super(orderedRows);
            this.endIndex = endIndex;
            cursor = startIndex;
        }

        @Override
        public boolean hasNext() {
            return cursor <= endIndex;
        }

        @Override
        public Value[] next() {
            if (cursor > endIndex) {
                throw new NoSuchElementException();
            }
            return orderedRows.get(cursor++);
        }

    }

    private static class PlainReverseItr extends Itr {

        final int startIndex;

        PlainReverseItr(ArrayList<Value[]> orderedRows, int startIndex, int endIndex) {
            super(orderedRows);
            this.startIndex = startIndex;
            cursor = endIndex;
        }

        @Override
        public boolean hasNext() {
            return cursor >= startIndex;
        }

        @Override
        public Value[] next() {
            if (cursor < startIndex) {
                throw new NoSuchElementException();
            }
            return orderedRows.get(cursor--);
        }

    }

    private static class BiItr extends PlainItr {

        final int end1, start1;

        BiItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2) {
            super(orderedRows, startIndex1, endIndex2);
            end1 = endIndex1;
            start1 = startIndex2;
        }

        @Override
        public Value[] next() {
            if (cursor > endIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != end1 ? cursor + 1 : start1;
            return r;
        }

    }

    private static class BiReverseItr extends PlainReverseItr {

        final int end1, start1;

        BiReverseItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2) {
            super(orderedRows, startIndex1, endIndex2);
            end1 = endIndex1;
            start1 = startIndex2;
        }

        @Override
        public Value[] next() {
            if (cursor < startIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != start1 ? cursor - 1 : end1;
            return r;
        }

    }

    private static final class TriItr extends BiItr {

        private final int end2, start2;

        TriItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2,
                int startIndex3, int endIndex3) {
            super(orderedRows, startIndex1, endIndex1, startIndex2, endIndex3);
            end2 = endIndex2;
            start2 = startIndex3;
        }

        @Override
        public Value[] next() {
            if (cursor > endIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != end1 ? cursor != end2 ? cursor + 1 : start2 : start1;
            return r;
        }

    }

    private static final class TriReverseItr extends BiReverseItr {

        private final int end2, start2;

        TriReverseItr(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1, int startIndex2, int endIndex2,
                int startIndex3, int endIndex3) {
            super(orderedRows, startIndex1, endIndex1, startIndex2, endIndex3);
            end2 = endIndex2;
            start2 = startIndex3;
        }

        @Override
        public Value[] next() {
            if (cursor < startIndex) {
                throw new NoSuchElementException();
            }
            Value[] r = orderedRows.get(cursor);
            cursor = cursor != start1 ? cursor != start2 ? cursor - 1 : end2 : end1;
            return r;
        }

    }

    private final WindowFrameUnits units;

    private final WindowFrameBound starting;

    private final WindowFrameBound following;

    private final WindowFrameExclusion exclusion;

    /**
     * Returns iterator for the specified frame, or default iterator if frame is
     * null.
     *
     * @param over
     *            window
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @param reverse
     *            whether iterator should iterate in reverse order
     * @return iterator
     */
    public static Iterator<Value[]> iterator(Window over, Session session, ArrayList<Value[]> orderedRows,
            SortOrder sortOrder, int currentRow, boolean reverse) {
        WindowFrame frame = over.getWindowFrame();
        if (frame != null) {
            return frame.iterator(session, orderedRows, sortOrder, currentRow, reverse);
        }
        int endIndex = orderedRows.size() - 1;
        return plainIterator(orderedRows, 0,
                over.getOrderBy() == null ? endIndex : toGroupEnd(orderedRows, sortOrder, currentRow, endIndex),
                reverse);
    }

    /**
     * Returns end index for the specified frame, or default end index if frame
     * is null.
     *
     * @param over
     *            window
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @return end index
     * @throws UnsupportedOperationException
     *             if over is not null and its exclusion clause is not EXCLUDE
     *             NO OTHERS
     */
    public static int getEndIndex(Window over, Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder,
            int currentRow) {
        WindowFrame frame = over.getWindowFrame();
        if (frame != null) {
            return frame.getEndIndex(session, orderedRows, sortOrder, currentRow);
        }
        int endIndex = orderedRows.size() - 1;
        return over.getOrderBy() == null ? endIndex : toGroupEnd(orderedRows, sortOrder, currentRow, endIndex);
    }

    private static Iterator<Value[]> plainIterator(ArrayList<Value[]> orderedRows, int startIndex, int endIndex,
            boolean reverse) {
        if (endIndex < startIndex) {
            return Collections.emptyIterator();
        }
        return reverse ? new PlainReverseItr(orderedRows, startIndex, endIndex)
                : new PlainItr(orderedRows, startIndex, endIndex);
    }

    private static Iterator<Value[]> biIterator(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1,
            int startIndex2, int endIndex2, boolean reverse) {
        return reverse ? new BiReverseItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2)
                : new BiItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2);
    }

    private static Iterator<Value[]> triIterator(ArrayList<Value[]> orderedRows, int startIndex1, int endIndex1,
            int startIndex2, int endIndex2, int startIndex3, int endIndex3, boolean reverse) {
        return reverse ? new TriReverseItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2, //
                startIndex3, endIndex3)
                : new TriItr(orderedRows, startIndex1, endIndex1, startIndex2, endIndex2, startIndex3, endIndex3);
    }

    private static int toGroupStart(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int offset, int minOffset) {
        Value[] row = orderedRows.get(offset);
        while (offset > minOffset && sortOrder.compare(row, orderedRows.get(offset - 1)) == 0) {
            offset--;
        }
        return offset;
    }

    private static int toGroupEnd(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int offset, int maxOffset) {
        Value[] row = orderedRows.get(offset);
        while (offset < maxOffset && sortOrder.compare(row, orderedRows.get(offset + 1)) == 0) {
            offset++;
        }
        return offset;
    }

    private static int getIntOffset(WindowFrameBound bound, Value[] values, Session session) {
        Value v = bound.isVariable() ? values[bound.getExpressionIndex()] : bound.getValue().getValue(session);
        int value = v.getInt();
        if (v == ValueNull.INSTANCE || value < 0) {
            throw DbException.get(ErrorCode.INVALID_PRECEDING_OR_FOLLOWING_1, v.getTraceSQL());
        }
        return value;
    }

    /**
     * Appends bound value to the current row and produces row for comparison
     * operations.
     *
     * @param session
     *            the session
     * @param orderedRows
     *            rows in partition
     * @param sortOrder
     *            the sort order
     * @param currentRow
     *            index of the current row
     * @param bound
     *            window frame bound
     * @param add
     *            false for PRECEDING, true for FOLLOWING
     * @return row for comparison operations, or null if result is out of range
     *         and should be treated as UNLIMITED
     */
    private static Value[] getCompareRow(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder,
            int currentRow, WindowFrameBound bound, boolean add) {
        int sortIndex = sortOrder.getQueryColumnIndexes()[0];
        Value[] row = orderedRows.get(currentRow);
        Value currentValue = row[sortIndex];
        int type = currentValue.getValueType();
        Value newValue;
        Value range = getValueOffset(bound, orderedRows.get(currentRow), session);
        switch (type) {
        case Value.NULL:
            newValue = ValueNull.INSTANCE;
            break;
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
        case Value.DECIMAL:
        case Value.DOUBLE:
        case Value.FLOAT:
        case Value.TIME:
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            OpType opType = add ^ (sortOrder.getSortTypes()[0] & SortOrder.DESCENDING) != 0 ? OpType.PLUS
                    : OpType.MINUS;
            try {
                newValue = new BinaryOperation(opType, ValueExpression.get(currentValue), ValueExpression.get(range))
                        .optimize(session).getValue(session).convertTo(type);
            } catch (DbException ex) {
                switch (ex.getErrorCode()) {
                case ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1:
                case ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2:
                    return null;
                }
                throw ex;
            }
            break;
        default:
            throw DbException.getInvalidValueException("unsupported type of sort key for RANGE units",
                    currentValue.getTraceSQL());
        }
        Value[] newRow = row.clone();
        newRow[sortIndex] = newValue;
        return newRow;
    }

    private static Value getValueOffset(WindowFrameBound bound, Value[] values, Session session) {
        Value value = bound.isVariable() ? values[bound.getExpressionIndex()] : bound.getValue().getValue(session);
        if (value == ValueNull.INSTANCE || value.getSignum() < 0) {
            throw DbException.get(ErrorCode.INVALID_PRECEDING_OR_FOLLOWING_1, value.getTraceSQL());
        }
        return value;
    }

    /**
     * Creates new instance of window frame clause.
     *
     * @param units
     *            units
     * @param starting
     *            starting clause
     * @param following
     *            following clause
     * @param exclusion
     *            exclusion clause
     */
    public WindowFrame(WindowFrameUnits units, WindowFrameBound starting, WindowFrameBound following,
            WindowFrameExclusion exclusion) {
        this.units = units;
        this.starting = starting;
        if (following != null && following.getType() == WindowFrameBoundType.CURRENT_ROW) {
            following = null;
        }
        this.following = following;
        this.exclusion = exclusion;
    }

    /**
     * Returns the units.
     *
     * @return the units
     */
    public WindowFrameUnits getUnits() {
        return units;
    }

    /**
     * Returns the starting clause.
     *
     * @return the starting clause
     */
    public WindowFrameBound getStarting() {
        return starting;
    }

    /**
     * Returns the following clause.
     *
     * @return the following clause, or null
     */
    public WindowFrameBound getFollowing() {
        return following;
    }

    /**
     * Returns the exclusion clause.
     *
     * @return the exclusion clause
     */
    public WindowFrameExclusion getExclusion() {
        return exclusion;
    }

    /**
     * Checks validity of this frame.
     *
     * @return whether bounds of this frame valid
     */
    public boolean isValid() {
        WindowFrameBoundType s = starting.getType(),
                f = following != null ? following.getType() : WindowFrameBoundType.CURRENT_ROW;
        return s != WindowFrameBoundType.UNBOUNDED_FOLLOWING && f != WindowFrameBoundType.UNBOUNDED_PRECEDING
                && s.compareTo(f) <= 0;
    }

    /**
     * Check if bounds of this frame has variable expressions. This method may
     * be used only after {@link #optimize(Session)} invocation.
     *
     * @return if bounds of this frame has variable expressions
     */
    public boolean isVariableBounds() {
        if (starting.isVariable()) {
            return true;
        }
        if (following != null && following.isVariable()) {
            return true;
        }
        return false;
    }

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver
     *            the column resolver
     * @param level
     *            the subquery nesting level
     * @param state
     *            current state for nesting checks
     */
    void mapColumns(ColumnResolver resolver, int level, int state) {
        starting.mapColumns(resolver, level, state);
        if (following != null) {
            following.mapColumns(resolver, level, state);
        }
    }

    /**
     * Try to optimize bound expressions.
     *
     * @param session
     *            the session
     */
    void optimize(Session session) {
        starting.optimize(session);
        if (following != null) {
            following.optimize(session);
        }
    }

    /**
     * Update an aggregate value.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     * @see Expression#updateAggregate(Session, int)
     */
    void updateAggregate(Session session, int stage) {
        starting.updateAggregate(session, stage);
        if (following != null) {
            following.updateAggregate(session, stage);
        }
    }

    /**
     * Returns iterator.
     *
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @param reverse
     *            whether iterator should iterate in reverse order
     * @return iterator
     */
    public Iterator<Value[]> iterator(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder,
            int currentRow, boolean reverse) {
        int startIndex = getIndex(session, orderedRows, sortOrder, currentRow, starting, false);
        int endIndex = following != null ? getIndex(session, orderedRows, sortOrder, currentRow, following, true)
                : units == WindowFrameUnits.ROWS ? currentRow
                        : toGroupEnd(orderedRows, sortOrder, currentRow, orderedRows.size() - 1);
        if (endIndex < startIndex) {
            return Collections.emptyIterator();
        }
        int size = orderedRows.size();
        if (startIndex >= size || endIndex < 0) {
            return Collections.emptyIterator();
        }
        if (startIndex < 0) {
            startIndex = 0;
        }
        if (endIndex >= size) {
            endIndex = size - 1;
        }
        return exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS
                ? complexIterator(orderedRows, sortOrder, currentRow, startIndex, endIndex, reverse)
                : plainIterator(orderedRows, startIndex, endIndex, reverse);
    }

    /**
     * Returns start index of this frame,
     *
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @return start index
     * @throws UnsupportedOperationException
     *             if exclusion clause is not EXCLUDE NO OTHERS
     */
    public int getStartIndex(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow) {
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            throw new UnsupportedOperationException();
        }
        int startIndex = getIndex(session, orderedRows, sortOrder, currentRow, starting, false);
        if (startIndex < 0) {
            startIndex = 0;
        }
        return startIndex;
    }

    /**
     * Returns end index of this frame,
     *
     * @param session
     *            the session
     * @param orderedRows
     *            ordered rows
     * @param sortOrder
     *            sort order
     * @param currentRow
     *            index of the current row
     * @return end index
     * @throws UnsupportedOperationException
     *             if exclusion clause is not EXCLUDE NO OTHERS
     */
    private int getEndIndex(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow) {
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            throw new UnsupportedOperationException();
        }
        int endIndex = following != null ? getIndex(session, orderedRows, sortOrder, currentRow, following, true)
                : units == WindowFrameUnits.ROWS ? currentRow
                        : toGroupEnd(orderedRows, sortOrder, currentRow, orderedRows.size() - 1);
        int size = orderedRows.size();
        if (endIndex >= size) {
            endIndex = size - 1;
        }
        return endIndex;
    }

    /**
     * Returns starting or ending index of a window frame.
     *
     * @param session
     *            the session
     * @param orderedRows
     *            rows in partition
     * @param sortOrder
     *            the sort order
     * @param currentRow
     *            index of the current row
     * @param bound
     *            window frame bound
     * @param forFollowing
     *            false for start index, true for end index
     * @return starting or ending index of a window frame (inclusive), can be 0
     *         or be equal to the number of rows if frame is not limited from
     *         that side
     */
    private int getIndex(Session session, ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow,
            WindowFrameBound bound, boolean forFollowing) {
        int size = orderedRows.size();
        int last = size - 1;
        int index;
        switch (bound.getType()) {
        case UNBOUNDED_PRECEDING:
            index = -1;
            break;
        case PRECEDING:
            switch (units) {
            case ROWS: {
                int value = getIntOffset(bound, orderedRows.get(currentRow), session);
                index = value > currentRow ? -1 : currentRow - value;
                break;
            }
            case GROUPS: {
                int value = getIntOffset(bound, orderedRows.get(currentRow), session);
                if (!forFollowing) {
                    index = toGroupStart(orderedRows, sortOrder, currentRow, 0);
                    while (value > 0 && index > 0) {
                        value--;
                        index = toGroupStart(orderedRows, sortOrder, index - 1, 0);
                    }
                    if (value > 0) {
                        index = -1;
                    }
                } else {
                    if (value == 0) {
                        index = toGroupEnd(orderedRows, sortOrder, currentRow, last);
                    } else {
                        index = currentRow;
                        while (value > 0 && index >= 0) {
                            value--;
                            index = toGroupStart(orderedRows, sortOrder, index, 0) - 1;
                        }
                    }
                }
                break;
            }
            case RANGE: {
                index = currentRow;
                Value[] row = getCompareRow(session, orderedRows, sortOrder, index, bound, false);
                if (row != null) {
                    index = Collections.binarySearch(orderedRows, row, sortOrder);
                    if (index >= 0) {
                        if (!forFollowing) {
                            while (index > 0 && sortOrder.compare(row, orderedRows.get(index - 1)) == 0) {
                                index--;
                            }
                        } else {
                            while (index < last && sortOrder.compare(row, orderedRows.get(index + 1)) == 0) {
                                index++;
                            }
                        }
                    } else {
                        index = ~index;
                        if (!forFollowing) {
                            if (index == 0) {
                                index = -1;
                            }
                        } else {
                            index--;
                        }
                    }
                } else {
                    index = -1;
                }
                break;
            }
            default:
                throw DbException.getUnsupportedException("units=" + units);
            }
            break;
        case CURRENT_ROW:
            switch (units) {
            case ROWS:
                index = currentRow;
                break;
            case GROUPS:
            case RANGE:
                index = forFollowing ? toGroupEnd(orderedRows, sortOrder, currentRow, last)
                        : toGroupStart(orderedRows, sortOrder, currentRow, 0);
                break;
            default:
                throw DbException.getUnsupportedException("units=" + units);
            }
            break;
        case FOLLOWING:
            switch (units) {
            case ROWS: {
                int value = getIntOffset(bound, orderedRows.get(currentRow), session);
                int rem = last - currentRow;
                index = value > rem ? size : currentRow + value;
                break;
            }
            case GROUPS: {
                int value = getIntOffset(bound, orderedRows.get(currentRow), session);
                if (forFollowing) {
                    index = toGroupEnd(orderedRows, sortOrder, currentRow, last);
                    while (value > 0 && index < last) {
                        value--;
                        index = toGroupEnd(orderedRows, sortOrder, index + 1, last);
                    }
                    if (value > 0) {
                        index = size;
                    }
                } else {
                    if (value == 0) {
                        index = toGroupStart(orderedRows, sortOrder, currentRow, 0);
                    } else {
                        index = currentRow;
                        while (value > 0 && index <= last) {
                            value--;
                            index = toGroupEnd(orderedRows, sortOrder, index, last) + 1;
                        }
                    }
                }
                break;
            }
            case RANGE: {
                index = currentRow;
                Value[] row = getCompareRow(session, orderedRows, sortOrder, index, bound, true);
                if (row != null) {
                    index = Collections.binarySearch(orderedRows, row, sortOrder);
                    if (index >= 0) {
                        if (forFollowing) {
                            while (index < last && sortOrder.compare(row, orderedRows.get(index + 1)) == 0) {
                                index++;
                            }
                        } else {
                            while (index > 0 && sortOrder.compare(row, orderedRows.get(index - 1)) == 0) {
                                index--;
                            }
                        }
                    } else {
                        index = ~index;
                        if (forFollowing) {
                            if (index != size) {
                                index--;
                            }
                        }
                    }
                } else {
                    index = size;
                }
                break;
            }
            default:
                throw DbException.getUnsupportedException("units=" + units);
            }
            break;
        case UNBOUNDED_FOLLOWING:
            index = size;
            break;
        default:
            throw DbException.getUnsupportedException("window frame bound type=" + bound.getType());
        }
        return index;
    }

    private Iterator<Value[]> complexIterator(ArrayList<Value[]> orderedRows, SortOrder sortOrder, int currentRow,
            int startIndex, int endIndex, boolean reverse) {
        if (exclusion == WindowFrameExclusion.EXCLUDE_CURRENT_ROW) {
            if (currentRow < startIndex || currentRow > endIndex) {
                // Nothing to exclude
            } else if (currentRow == startIndex) {
                startIndex++;
            } else if (currentRow == endIndex) {
                endIndex--;
            } else {
                return biIterator(orderedRows, startIndex, currentRow - 1, currentRow + 1, endIndex, reverse);
            }
        } else {
            // Do not include previous rows if they are not in the range
            int exStart = toGroupStart(orderedRows, sortOrder, currentRow, startIndex);
            // Do not include next rows if they are not in the range
            int exEnd = toGroupEnd(orderedRows, sortOrder, currentRow, endIndex);
            boolean includeCurrentRow = exclusion == WindowFrameExclusion.EXCLUDE_TIES;
            if (includeCurrentRow) {
                // Simplify exclusion if possible
                if (currentRow == exStart) {
                    exStart++;
                    includeCurrentRow = false;
                } else if (currentRow == exEnd) {
                    exEnd--;
                    includeCurrentRow = false;
                }
            }
            if (exStart > exEnd || exEnd < startIndex || exStart > endIndex) {
                // Empty range or nothing to exclude
            } else if (includeCurrentRow) {
                if (startIndex == exStart) {
                    if (endIndex == exEnd) {
                        return Collections.singleton(orderedRows.get(currentRow)).iterator();
                    } else {
                        return biIterator(orderedRows, currentRow, currentRow, exEnd + 1, endIndex, reverse);
                    }
                } else {
                    if (endIndex == exEnd) {
                        return biIterator(orderedRows, startIndex, exStart - 1, currentRow, currentRow, reverse);
                    } else {
                        return triIterator(orderedRows, startIndex, exStart - 1, currentRow, currentRow, exEnd + 1,
                                endIndex, reverse);
                    }
                }
            } else {
                if (startIndex >= exStart) {
                    startIndex = exEnd + 1;
                } else if (endIndex <= exEnd) {
                    endIndex = exStart - 1;
                } else {
                    return biIterator(orderedRows, startIndex, exStart - 1, exEnd + 1, endIndex, reverse);
                }
            }
        }
        return plainIterator(orderedRows, startIndex, endIndex, reverse);
    }

    /**
     * Append SQL representation to the specified builder.
     *
     * @param builder
     *            string builder
     * @param alwaysQuote
     *            quote all identifiers
     * @return the specified string builder
     * @see org.h2.expression.Expression#getSQL(StringBuilder, boolean)
     */
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append(units.getSQL());
        if (following == null) {
            builder.append(' ');
            starting.getSQL(builder, false, alwaysQuote);
        } else {
            builder.append(" BETWEEN ");
            starting.getSQL(builder, false, alwaysQuote).append(" AND ");
            following.getSQL(builder, true, alwaysQuote);
        }
        if (exclusion != WindowFrameExclusion.EXCLUDE_NO_OTHERS) {
            builder.append(' ').append(exclusion.getSQL());
        }
        return builder;
    }

}
