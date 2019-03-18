/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 */
public class Aggregate extends Expression {

    public enum AggregateType {
        /**
         * The aggregate type for COUNT(*).
         */
        COUNT_ALL,

        /**
         * The aggregate type for COUNT(expression).
         */
        COUNT,

        /**
         * The aggregate type for GROUP_CONCAT(...).
         */
        GROUP_CONCAT,

        /**
         * The aggregate type for SUM(expression).
         */
        SUM,

        /**
         * The aggregate type for MIN(expression).
         */
        MIN,

        /**
         * The aggregate type for MAX(expression).
         */
        MAX,

        /**
         * The aggregate type for AVG(expression).
         */
        AVG,

        /**
         * The aggregate type for STDDEV_POP(expression).
         */
        STDDEV_POP,

        /**
         * The aggregate type for STDDEV_SAMP(expression).
         */
        STDDEV_SAMP,

        /**
         * The aggregate type for VAR_POP(expression).
         */
        VAR_POP,

        /**
         * The aggregate type for VAR_SAMP(expression).
         */
        VAR_SAMP,

        /**
         * The aggregate type for BOOL_OR(expression).
         */
        BOOL_OR,

        /**
         * The aggregate type for BOOL_AND(expression).
         */
        BOOL_AND,

        /**
         * The aggregate type for BOOL_OR(expression).
         */
        BIT_OR,

        /**
         * The aggregate type for BOOL_AND(expression).
         */
        BIT_AND,

        /**
         * The aggregate type for SELECTIVITY(expression).
         */
        SELECTIVITY,

        /**
         * The aggregate type for HISTOGRAM(expression).
         */
        HISTOGRAM,

        /**
         * The aggregate type for MEDIAN(expression).
         */
        MEDIAN,
        /**
         * The aggregate type for ARRAY_AGG(expression).
         */
        ARRAY_AGG
    }

    private static final HashMap<String, AggregateType> AGGREGATES = new HashMap<>(26);

    private final AggregateType type;
    private final Select select;
    private final boolean distinct;

    private Expression on;
    private Expression groupConcatSeparator;
    private ArrayList<SelectOrderBy> groupConcatOrderList;
    private ArrayList<SelectOrderBy> arrayAggOrderList;
    private SortOrder groupConcatSort;
    private SortOrder arrayOrderSort;
    private int dataType, scale;
    private long precision;
    private int displaySize;
    private int lastGroupRowId;

    private Expression filterCondition;

    /**
     * Create a new aggregate object.
     *
     * @param type the aggregate type
     * @param on the aggregated expression
     * @param select the select statement
     * @param distinct if distinct is used
     */
    public Aggregate(AggregateType type, Expression on, Select select, boolean distinct) {
        this.type = type;
        this.on = on;
        this.select = select;
        this.distinct = distinct;
    }

    static {
        /*
         * Update initial size of AGGREGATES after editing the following list.
         */
        addAggregate("COUNT", AggregateType.COUNT);
        addAggregate("SUM", AggregateType.SUM);
        addAggregate("MIN", AggregateType.MIN);
        addAggregate("MAX", AggregateType.MAX);
        addAggregate("AVG", AggregateType.AVG);
        addAggregate("GROUP_CONCAT", AggregateType.GROUP_CONCAT);
        // PostgreSQL compatibility: string_agg(expression, delimiter)
        addAggregate("STRING_AGG", AggregateType.GROUP_CONCAT);
        addAggregate("STDDEV_SAMP", AggregateType.STDDEV_SAMP);
        addAggregate("STDDEV", AggregateType.STDDEV_SAMP);
        addAggregate("STDDEV_POP", AggregateType.STDDEV_POP);
        addAggregate("STDDEVP", AggregateType.STDDEV_POP);
        addAggregate("VAR_POP", AggregateType.VAR_POP);
        addAggregate("VARP", AggregateType.VAR_POP);
        addAggregate("VAR_SAMP", AggregateType.VAR_SAMP);
        addAggregate("VAR", AggregateType.VAR_SAMP);
        addAggregate("VARIANCE", AggregateType.VAR_SAMP);
        addAggregate("BOOL_OR", AggregateType.BOOL_OR);
        // HSQLDB compatibility, but conflicts with x > EVERY(...)
        addAggregate("SOME", AggregateType.BOOL_OR);
        addAggregate("BOOL_AND", AggregateType.BOOL_AND);
        // HSQLDB compatibility, but conflicts with x > SOME(...)
        addAggregate("EVERY", AggregateType.BOOL_AND);
        addAggregate("SELECTIVITY", AggregateType.SELECTIVITY);
        addAggregate("HISTOGRAM", AggregateType.HISTOGRAM);
        addAggregate("BIT_OR", AggregateType.BIT_OR);
        addAggregate("BIT_AND", AggregateType.BIT_AND);
        addAggregate("MEDIAN", AggregateType.MEDIAN);
        addAggregate("ARRAY_AGG", AggregateType.ARRAY_AGG);
    }

    private static void addAggregate(String name, AggregateType type) {
        AGGREGATES.put(name, type);
    }

    /**
     * Get the aggregate type for this name, or -1 if no aggregate has been
     * found.
     *
     * @param name the aggregate function name
     * @return null if no aggregate function has been found, or the aggregate type
     */
    public static AggregateType getAggregateType(String name) {
        return AGGREGATES.get(name);
    }

    /**
     * Set the order for GROUP_CONCAT() aggregate.
     *
     * @param orderBy the order by list
     */
    public void setGroupConcatOrder(ArrayList<SelectOrderBy> orderBy) {
        this.groupConcatOrderList = orderBy;
    }

    /**
     * Set the order for ARRAY_AGG() aggregate.
     *
     * @param orderBy the order by list
     */
    public void setArrayAggOrder(ArrayList<SelectOrderBy> orderBy) {
        this.arrayAggOrderList = orderBy;
    }

    /**
     * Set the separator for the GROUP_CONCAT() aggregate.
     *
     * @param separator the separator expression
     */
    public void setGroupConcatSeparator(Expression separator) {
        this.groupConcatSeparator = separator;
    }

    /**
     * Sets the FILTER condition.
     *
     * @param filterCondition condition
     */
    public void setFilterCondition(Expression filterCondition) {
        this.filterCondition = filterCondition;
    }

    private SortOrder initOrder(ArrayList<SelectOrderBy> orderList, Session session) {
        int size = orderList.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = orderList.get(i);
            index[i] = i + 1;
            int order = o.descending ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            sortType[i] = order;
        }
        return new SortOrder(session.getDatabase(), index, sortType, null);
    }

    @Override
    public void updateAggregate(Session session) {
        // TODO aggregates: check nested MIN(MAX(ID)) and so on
        // if (on != null) {
        // on.updateAggregate();
        // }
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = AggregateData.create(type);
            group.put(this, data);
        }
        Value v = on == null ? null : on.getValue(session);
        if (type == AggregateType.GROUP_CONCAT) {
            if (v != ValueNull.INSTANCE) {
                v = v.convertTo(Value.STRING);
                if (groupConcatOrderList != null) {
                    int size = groupConcatOrderList.size();
                    Value[] array = new Value[1 + size];
                    array[0] = v;
                    for (int i = 0; i < size; i++) {
                        SelectOrderBy o = groupConcatOrderList.get(i);
                        array[i + 1] = o.expression.getValue(session);
                    }
                    v = ValueArray.get(array);
                }
            }
        }
        if (type == AggregateType.ARRAY_AGG) {
            if (v != ValueNull.INSTANCE) {
                if (arrayAggOrderList != null) {
                    int size = arrayAggOrderList.size();
                    Value[] array = new Value[1 + size];
                    array[0] = v;
                    for (int i = 0; i < size; i++) {
                        SelectOrderBy o = arrayAggOrderList.get(i);
                        array[i + 1] = o.expression.getValue(session);
                    }
                    v = ValueArray.get(array);
                }
            }
        }
        if (filterCondition != null) {
            if (!filterCondition.getBooleanValue(session)) {
                return;
            }
        }
        data.add(session.getDatabase(), dataType, distinct, v);
    }

    @Override
    public Value getValue(Session session) {
        if (select.isQuickAggregateQuery()) {
            switch (type) {
            case COUNT:
            case COUNT_ALL:
                Table table = select.getTopTableFilter().getTable();
                return ValueLong.get(table.getRowCount(session));
            case MIN:
            case MAX: {
                boolean first = type == AggregateType.MIN;
                Index index = getMinMaxColumnIndex();
                int sortType = index.getIndexColumns()[0].sortType;
                if ((sortType & SortOrder.DESCENDING) != 0) {
                    first = !first;
                }
                Cursor cursor = index.findFirstOrLast(session, first);
                SearchRow row = cursor.getSearchRow();
                Value v;
                if (row == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    v = row.getValue(index.getColumns()[0].getColumnId());
                }
                return v;
            }
            case MEDIAN: {
                return AggregateDataMedian.getResultFromIndex(session, on, dataType);
            }
            default:
                DbException.throwInternalError("type=" + type);
            }
        }
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = AggregateData.create(type);
        }
        Value v = data.getValue(session.getDatabase(), dataType, distinct);
        if (type == AggregateType.GROUP_CONCAT) {
            ArrayList<Value> list = ((AggregateDataArrayCollecting) data).getList();
            if (list == null || list.isEmpty()) {
                return ValueNull.INSTANCE;
            }
            if (groupConcatOrderList != null) {
                final SortOrder sortOrder = groupConcatSort;
                Collections.sort(list, new Comparator<Value>() {
                    @Override
                    public int compare(Value v1, Value v2) {
                        Value[] a1 = ((ValueArray) v1).getList();
                        Value[] a2 = ((ValueArray) v2).getList();
                        return sortOrder.compare(a1, a2);
                    }
                });
            }
            StatementBuilder buff = new StatementBuilder();
            String sep = groupConcatSeparator == null ?
                    "," : groupConcatSeparator.getValue(session).getString();
            for (Value val : list) {
                String s;
                if (val.getType() == Value.ARRAY) {
                    s = ((ValueArray) val).getList()[0].getString();
                } else {
                    s = val.getString();
                }
                if (s == null) {
                    continue;
                }
                if (sep != null) {
                    buff.appendExceptFirst(sep);
                }
                buff.append(s);
            }
            v = ValueString.get(buff.toString());
        } else if (type == AggregateType.ARRAY_AGG) {
            ArrayList<Value> list = ((AggregateDataArrayCollecting) data).getList();
            if (list == null || list.isEmpty()) {
                return ValueNull.INSTANCE;
            }
            if (arrayAggOrderList != null) {
                final SortOrder sortOrder = arrayOrderSort;
                Collections.sort(list, new Comparator<Value>() {
                    @Override
                    public int compare(Value v1, Value v2) {
                        Value[] a1 = ((ValueArray) v1).getList();
                        Value[] a2 = ((ValueArray) v2).getList();
                        return sortOrder.compare(a1, a2);
                    }
                });
            }
            v = ValueArray.get(list.toArray(new Value[list.size()]));
        }
        return v;
    }

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (on != null) {
            on.mapColumns(resolver, level);
        }
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression.mapColumns(resolver, level);
            }
        }
        if (arrayAggOrderList != null) {
            for (SelectOrderBy o : arrayAggOrderList) {
                o.expression.mapColumns(resolver, level);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.mapColumns(resolver, level);
        }
        if (filterCondition != null) {
            filterCondition.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(Session session) {
        if (on != null) {
            on = on.optimize(session);
            dataType = on.getType();
            scale = on.getScale();
            precision = on.getPrecision();
            displaySize = on.getDisplaySize();
        }
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression = o.expression.optimize(session);
            }
            groupConcatSort = initOrder(groupConcatOrderList, session);
        }
        if (arrayAggOrderList != null) {
            for (SelectOrderBy o : arrayAggOrderList) {
                o.expression = o.expression.optimize(session);
            }
            arrayOrderSort = initOrder(arrayAggOrderList, session);
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator = groupConcatSeparator.optimize(session);
        }
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
        }
        switch (type) {
        case GROUP_CONCAT:
            dataType = Value.STRING;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case COUNT_ALL:
        case COUNT:
            dataType = Value.LONG;
            scale = 0;
            precision = ValueLong.PRECISION;
            displaySize = ValueLong.DISPLAY_SIZE;
            break;
        case SELECTIVITY:
            dataType = Value.INT;
            scale = 0;
            precision = ValueInt.PRECISION;
            displaySize = ValueInt.DISPLAY_SIZE;
            break;
        case HISTOGRAM:
            dataType = Value.ARRAY;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case SUM:
            if (dataType == Value.BOOLEAN) {
                // example: sum(id > 3) (count the rows)
                dataType = Value.LONG;
            } else if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            } else {
                dataType = DataType.getAddProofType(dataType);
            }
            break;
        case AVG:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case MIN:
        case MAX:
        case MEDIAN:
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            dataType = Value.DOUBLE;
            precision = ValueDouble.PRECISION;
            displaySize = ValueDouble.DISPLAY_SIZE;
            scale = 0;
            break;
        case BOOL_AND:
        case BOOL_OR:
            dataType = Value.BOOLEAN;
            precision = ValueBoolean.PRECISION;
            displaySize = ValueBoolean.DISPLAY_SIZE;
            scale = 0;
            break;
        case BIT_AND:
        case BIT_OR:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case ARRAY_AGG:
            dataType = Value.ARRAY;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (on != null) {
            on.setEvaluatable(tableFilter, b);
        }
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression.setEvaluatable(tableFilter, b);
            }
        }
        if (arrayAggOrderList != null) {
            for (SelectOrderBy o : arrayAggOrderList) {
                o.expression.setEvaluatable(tableFilter, b);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.setEvaluatable(tableFilter, b);
        }
        if (filterCondition != null) {
            filterCondition.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public long getPrecision() {
        return precision;
    }

    @Override
    public int getDisplaySize() {
        return displaySize;
    }

    private String getSQLGroupConcat() {
        StatementBuilder buff = new StatementBuilder("GROUP_CONCAT(");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(on.getSQL());
        if (groupConcatOrderList != null) {
            buff.append(" ORDER BY ");
            for (SelectOrderBy o : groupConcatOrderList) {
                buff.appendExceptFirst(", ");
                buff.append(o.expression.getSQL());
                if (o.descending) {
                    buff.append(" DESC");
                }
            }
        }
        if (groupConcatSeparator != null) {
            buff.append(" SEPARATOR ").append(groupConcatSeparator.getSQL());
        }
        buff.append(')');
        if (filterCondition != null) {
            buff.append(" FILTER (WHERE ").append(filterCondition.getSQL()).append(')');
        }
        return buff.toString();
    }

    private String getSQLArrayAggregate() {
        StatementBuilder buff = new StatementBuilder("ARRAY_AGG(");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(on.getSQL());
        if (arrayAggOrderList != null) {
            buff.append(" ORDER BY ");
            for (SelectOrderBy o : arrayAggOrderList) {
                buff.appendExceptFirst(", ");
                buff.append(o.expression.getSQL());
                if (o.descending) {
                    buff.append(" DESC");
                }
            }
        }
        buff.append(')');
        if (filterCondition != null) {
            buff.append(" FILTER (WHERE ").append(filterCondition.getSQL()).append(')');
        }
        return buff.toString();
    }

    @Override
    public String getSQL() {
        String text;
        switch (type) {
        case GROUP_CONCAT:
            return getSQLGroupConcat();
        case COUNT_ALL:
            return "COUNT(*)";
        case COUNT:
            text = "COUNT";
            break;
        case SELECTIVITY:
            text = "SELECTIVITY";
            break;
        case HISTOGRAM:
            text = "HISTOGRAM";
            break;
        case SUM:
            text = "SUM";
            break;
        case MIN:
            text = "MIN";
            break;
        case MAX:
            text = "MAX";
            break;
        case AVG:
            text = "AVG";
            break;
        case STDDEV_POP:
            text = "STDDEV_POP";
            break;
        case STDDEV_SAMP:
            text = "STDDEV_SAMP";
            break;
        case VAR_POP:
            text = "VAR_POP";
            break;
        case VAR_SAMP:
            text = "VAR_SAMP";
            break;
        case BOOL_AND:
            text = "BOOL_AND";
            break;
        case BOOL_OR:
            text = "BOOL_OR";
            break;
        case BIT_AND:
            text = "BIT_AND";
            break;
        case BIT_OR:
            text = "BIT_OR";
            break;
        case MEDIAN:
            text = "MEDIAN";
            break;
        case ARRAY_AGG:
            return getSQLArrayAggregate();
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        if (distinct) {
            text += "(DISTINCT " + on.getSQL() + ')';
        } else {
            text += StringUtils.enclose(on.getSQL());
        }
        if (filterCondition != null) {
            text += " FILTER (WHERE " + filterCondition.getSQL() + ')';
        }
        return text;
    }

    private Index getMinMaxColumnIndex() {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            TableFilter filter = col.getTableFilter();
            if (filter != null) {
                Table table = filter.getTable();
                return table.getIndexForColumn(column, true, false);
            }
        }
        return null;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (filterCondition != null && !filterCondition.isEverything(visitor)) {
            return false;
        }
        if (visitor.getType() == ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL) {
            switch (type) {
            case COUNT:
                if (!distinct && on.getNullable() == Column.NOT_NULLABLE) {
                    return visitor.getTable().canGetRowCount();
                }
                return false;
            case COUNT_ALL:
                return visitor.getTable().canGetRowCount();
            case MIN:
            case MAX:
                Index index = getMinMaxColumnIndex();
                return index != null;
            case MEDIAN:
                if (distinct) {
                    return false;
                }
                return AggregateDataMedian.getMedianColumnIndex(on) != null;
            default:
                return false;
            }
        }
        if (on != null && !on.isEverything(visitor)) {
            return false;
        }
        if (groupConcatSeparator != null &&
                !groupConcatSeparator.isEverything(visitor)) {
            return false;
        }
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                if (!o.expression.isEverything(visitor)) {
                    return false;
                }
            }
        }
        if (arrayAggOrderList != null) {
            for (SelectOrderBy o : arrayAggOrderList) {
                if (!o.expression.isEverything(visitor)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 1;
        if (on != null) {
            cost += on.getCost();
        }
        if (filterCondition != null) {
            cost += filterCondition.getCost();
        }
        return cost;
    }

}
