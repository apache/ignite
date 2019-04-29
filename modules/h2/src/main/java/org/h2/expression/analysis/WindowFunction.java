/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A window function.
 */
public class WindowFunction extends DataAnalysisOperation {

    private final WindowFunctionType type;

    private final Expression[] args;

    private boolean fromLast;

    private boolean ignoreNulls;

    /**
     * Returns minimal number of arguments for the specified type.
     *
     * @param type
     *            the type of a window function
     * @return minimal number of arguments
     */
    public static int getMinArgumentCount(WindowFunctionType type) {
        switch (type) {
        case NTILE:
        case LEAD:
        case LAG:
        case FIRST_VALUE:
        case LAST_VALUE:
        case RATIO_TO_REPORT:
            return 1;
        case NTH_VALUE:
            return 2;
        default:
            return 0;
        }
    }

    /**
     * Returns maximal number of arguments for the specified type.
     *
     * @param type
     *            the type of a window function
     * @return maximal number of arguments
     */
    public static int getMaxArgumentCount(WindowFunctionType type) {
        switch (type) {
        case NTILE:
        case FIRST_VALUE:
        case LAST_VALUE:
        case RATIO_TO_REPORT:
            return 1;
        case LEAD:
        case LAG:
            return 3;
        case NTH_VALUE:
            return 2;
        default:
            return 0;
        }
    }

    private static Value getNthValue(Iterator<Value[]> iterator, int number, boolean ignoreNulls) {
        Value v = ValueNull.INSTANCE;
        int cnt = 0;
        while (iterator.hasNext()) {
            Value t = iterator.next()[0];
            if (!ignoreNulls || t != ValueNull.INSTANCE) {
                if (cnt++ == number) {
                    v = t;
                    break;
                }
            }
        }
        return v;
    }

    /**
     * Creates new instance of a window function.
     *
     * @param type
     *            the type
     * @param select
     *            the select statement
     * @param args
     *            arguments, or null
     */
    public WindowFunction(WindowFunctionType type, Select select, Expression[] args) {
        super(select);
        this.type = type;
        this.args = args;
    }

    /**
     * Returns the type of this function.
     *
     * @return the type of this function
     */
    public WindowFunctionType getFunctionType() {
        return type;
    }

    /**
     * Sets FROM FIRST or FROM LAST clause value.
     *
     * @param fromLast
     *            whether FROM LAST clause was specified.
     */
    public void setFromLast(boolean fromLast) {
        this.fromLast = fromLast;
    }

    /**
     * Sets RESPECT NULLS or IGNORE NULLS clause value.
     *
     * @param ignoreNulls
     *            whether IGNORE NULLS clause was specified
     */
    public void setIgnoreNulls(boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    public boolean isAggregate() {
        return false;
    }

    @Override
    protected void updateAggregate(Session session, SelectGroups groupData, int groupRowId) {
        updateOrderedAggregate(session, groupData, groupRowId, over.getOrderBy());
    }

    @Override
    protected void updateGroupAggregates(Session session, int stage) {
        super.updateGroupAggregates(session, stage);
        if (args != null) {
            for (Expression expr : args) {
                expr.updateAggregate(session, stage);
            }
        }
    }

    @Override
    protected int getNumExpressions() {
        return args != null ? args.length : 0;
    }

    @Override
    protected void rememberExpressions(Session session, Value[] array) {
        if (args != null) {
            for (int i = 0, cnt = args.length; i < cnt; i++) {
                array[i] = args[i].getValue(session);
            }
        }
    }

    @Override
    protected Object createAggregateData() {
        throw DbException.getUnsupportedException("Window function");
    }

    @Override
    protected void getOrderedResultLoop(Session session, HashMap<Integer, Value> result, ArrayList<Value[]> ordered,
            int rowIdColumn) {
        switch (type) {
        case ROW_NUMBER:
            for (int i = 0, size = ordered.size(); i < size;) {
                result.put(ordered.get(i)[rowIdColumn].getInt(), ValueLong.get(++i));
            }
            break;
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
            getRank(result, ordered, rowIdColumn);
            break;
        case CUME_DIST:
            getCumeDist(result, ordered, rowIdColumn);
            break;
        case NTILE:
            getNtile(result, ordered, rowIdColumn);
            break;
        case LEAD:
        case LAG:
            getLeadLag(result, ordered, rowIdColumn);
            break;
        case FIRST_VALUE:
        case LAST_VALUE:
        case NTH_VALUE:
            getNth(session, result, ordered, rowIdColumn);
            break;
        case RATIO_TO_REPORT:
            getRatioToReport(result, ordered, rowIdColumn);
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    private void getRank(HashMap<Integer, Value> result, ArrayList<Value[]> ordered, int rowIdColumn) {
        int size = ordered.size();
        int number = 0;
        for (int i = 0; i < size; i++) {
            Value[] row = ordered.get(i);
            if (i == 0) {
                number = 1;
            } else if (getOverOrderBySort().compare(ordered.get(i - 1), row) != 0) {
                if (type == WindowFunctionType.DENSE_RANK) {
                    number++;
                } else {
                    number = i + 1;
                }
            }
            Value v;
            if (type == WindowFunctionType.PERCENT_RANK) {
                int nm = number - 1;
                v = nm == 0 ? ValueDouble.ZERO : ValueDouble.get((double) nm / (size - 1));
            } else {
                v = ValueLong.get(number);
            }
            result.put(row[rowIdColumn].getInt(), v);
        }
    }

    private void getCumeDist(HashMap<Integer, Value> result, ArrayList<Value[]> orderedData, int rowIdColumn) {
        int size = orderedData.size();
        for (int start = 0; start < size;) {
            Value[] array = orderedData.get(start);
            int end = start + 1;
            while (end < size && overOrderBySort.compare(array, orderedData.get(end)) == 0) {
                end++;
            }
            ValueDouble v = ValueDouble.get((double) end / size);
            for (int i = start; i < end; i++) {
                int rowId = orderedData.get(i)[rowIdColumn].getInt();
                result.put(rowId, v);
            }
            start = end;
        }
    }

    private static void getNtile(HashMap<Integer, Value> result, ArrayList<Value[]> orderedData, int rowIdColumn) {
        int size = orderedData.size();
        for (int i = 0; i < size; i++) {
            Value[] array = orderedData.get(i);
            long buckets = array[0].getLong();
            if (buckets <= 0) {
                throw DbException.getInvalidValueException("number of tiles", buckets);
            }
            long perTile = size / buckets;
            long numLarger = size - perTile * buckets;
            long largerGroup = numLarger * (perTile + 1);
            long v;
            if (i >= largerGroup) {
                v = (i - largerGroup) / perTile + numLarger + 1;
            } else {
                v = i / (perTile + 1) + 1;
            }
            result.put(orderedData.get(i)[rowIdColumn].getInt(), ValueLong.get(v));
        }
    }

    private void getLeadLag(HashMap<Integer, Value> result, ArrayList<Value[]> ordered, int rowIdColumn) {
        int size = ordered.size();
        int numExpressions = getNumExpressions();
        int dataType = args[0].getType().getValueType();
        for (int i = 0; i < size; i++) {
            Value[] row = ordered.get(i);
            int rowId = row[rowIdColumn].getInt();
            int n;
            if (numExpressions >= 2) {
                n = row[1].getInt();
                // 0 is valid here
                if (n < 0) {
                    throw DbException.getInvalidValueException("nth row", n);
                }
            } else {
                n = 1;
            }
            Value v = null;
            if (n == 0) {
                v = ordered.get(i)[0];
            } else if (type == WindowFunctionType.LEAD) {
                if (ignoreNulls) {
                    for (int j = i + 1; n > 0 && j < size; j++) {
                        v = ordered.get(j)[0];
                        if (v != ValueNull.INSTANCE) {
                            n--;
                        }
                    }
                    if (n > 0) {
                        v = null;
                    }
                } else {
                    if (n <= size - i - 1) {
                        v = ordered.get(i + n)[0];
                    }
                }
            } else /* LAG */ {
                if (ignoreNulls) {
                    for (int j = i - 1; n > 0 && j >= 0; j--) {
                        v = ordered.get(j)[0];
                        if (v != ValueNull.INSTANCE) {
                            n--;
                        }
                    }
                    if (n > 0) {
                        v = null;
                    }
                } else {
                    if (n <= i) {
                        v = ordered.get(i - n)[0];
                    }
                }
            }
            if (v == null) {
                if (numExpressions >= 3) {
                    v = row[2].convertTo(dataType);
                } else {
                    v = ValueNull.INSTANCE;
                }
            }
            result.put(rowId, v);
        }
    }

    private void getNth(Session session, HashMap<Integer, Value> result, ArrayList<Value[]> ordered, int rowIdColumn) {
        int size = ordered.size();
        for (int i = 0; i < size; i++) {
            Value[] row = ordered.get(i);
            int rowId = row[rowIdColumn].getInt();
            Value v;
            switch (type) {
            case FIRST_VALUE:
                v = getNthValue(WindowFrame.iterator(over, session, ordered, getOverOrderBySort(), i, false), 0,
                        ignoreNulls);
                break;
            case LAST_VALUE:
                v = getNthValue(WindowFrame.iterator(over, session, ordered, getOverOrderBySort(), i, true), 0,
                        ignoreNulls);
                break;
            case NTH_VALUE: {
                int n = row[1].getInt();
                if (n <= 0) {
                    throw DbException.getInvalidValueException("nth row", n);
                }
                n--;
                Iterator<Value[]> iter = WindowFrame.iterator(over, session, ordered, getOverOrderBySort(), i,
                        fromLast);
                v = getNthValue(iter, n, ignoreNulls);
                break;
            }
            default:
                throw DbException.throwInternalError("type=" + type);
            }
            result.put(rowId, v);
        }
    }

    private static void getRatioToReport(HashMap<Integer, Value> result, ArrayList<Value[]> ordered, int rowIdColumn) {
        int size = ordered.size();
        Value value = null;
        for (int i = 0; i < size; i++) {
            Value v = ordered.get(i)[0];
            if (v != ValueNull.INSTANCE) {
                if (value == null) {
                    value = v.convertTo(Value.DOUBLE);
                } else {
                    value = value.add(v.convertTo(Value.DOUBLE));
                }
            }
        }
        if (value != null && value.getSignum() == 0) {
            value = null;
        }
        for (int i = 0; i < size; i++) {
            Value[] row = ordered.get(i);
            Value v;
            if (value == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = row[0];
                if (v != ValueNull.INSTANCE) {
                    v = v.convertTo(Value.DOUBLE).divide(value);
                }
            }
            result.put(row[rowIdColumn].getInt(), v);
        }
    }

    @Override
    protected Value getAggregatedValue(Session session, Object aggregateData) {
        throw DbException.getUnsupportedException("Window function");
    }

    @Override
    public void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (args != null) {
            for (Expression arg : args) {
                arg.mapColumns(resolver, level, innerState);
            }
        }
        super.mapColumnsAnalysis(resolver, level, innerState);
    }

    @Override
    public Expression optimize(Session session) {
        if (over.getWindowFrame() != null) {
            switch (type) {
            case FIRST_VALUE:
            case LAST_VALUE:
            case NTH_VALUE:
                break;
            default:
                String sql = getSQL(false);
                throw DbException.getSyntaxError(sql, sql.length() - 1);
            }
        }
        if (over.getOrderBy() == null) {
            switch (type) {
            case RANK:
            case DENSE_RANK:
            case NTILE:
            case LEAD:
            case LAG:
                String sql = getSQL(false);
                throw DbException.getSyntaxError(sql, sql.length() - 1, "ORDER BY");
            default:
            }
        } else if (type == WindowFunctionType.RATIO_TO_REPORT) {
            String sql = getSQL(false);
            throw DbException.getSyntaxError(sql, sql.length() - 1);
        }
        super.optimize(session);
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                args[i] = args[i].optimize(session);
            }
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (args != null) {
            for (Expression e : args) {
                e.setEvaluatable(tableFilter, b);
            }
        }
        super.setEvaluatable(tableFilter, b);
    }

    @Override
    public TypeInfo getType() {
        switch (type) {
        case ROW_NUMBER:
        case RANK:
        case DENSE_RANK:
        case NTILE:
            return TypeInfo.TYPE_LONG;
        case PERCENT_RANK:
        case CUME_DIST:
        case RATIO_TO_REPORT:
            return TypeInfo.TYPE_DOUBLE;
        case LEAD:
        case LAG:
        case FIRST_VALUE:
        case LAST_VALUE:
        case NTH_VALUE:
            return args[0].getType();
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        String name = type.getSQL();
        builder.append(name).append('(');
        if (args != null) {
            writeExpressions(builder, args, alwaysQuote);
        }
        builder.append(')');
        if (fromLast && type == WindowFunctionType.NTH_VALUE) {
            builder.append(" FROM LAST");
        }
        if (ignoreNulls) {
            switch (type) {
            case LEAD:
            case LAG:
            case FIRST_VALUE:
            case LAST_VALUE:
            case NTH_VALUE:
                builder.append(" IGNORE NULLS");
                //$FALL-THROUGH$
            default:
            }
        }
        return appendTailConditions(builder, alwaysQuote);
    }

    @Override
    public int getCost() {
        int cost = 1;
        if (args != null) {
            for (Expression expr : args) {
                cost += expr.getCost();
            }
        }
        return cost;
    }

}
