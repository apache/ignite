/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * Implementation of the functions TABLE(..), TABLE_DISTINCT(..), and
 * UNNEST(..).
 */
public class TableFunction extends Function {
    private final long rowCount;
    private Column[] columns;

    TableFunction(Database database, FunctionInfo info, long rowCount) {
        super(database, info);
        this.rowCount = rowCount;
    }

    @Override
    public Value getValue(Session session) {
        return getTable(session, false);
    }

    @Override
    protected void checkParameterCount(int len) {
        if (len < 1) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, getName(), ">0");
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        if (info.type == UNNEST) {
            super.getSQL(builder, alwaysQuote);
            if (args.length < columns.length) {
                builder.append(" WITH ORDINALITY");
            }
            return builder;
        }
        builder.append(getName()).append('(');
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(columns[i].getCreateSQL()).append('=');
            args[i].getSQL(builder, alwaysQuote);
        }
        return builder.append(')');
    }

    @Override
    public ValueResultSet getValueForColumnList(Session session,
            Expression[] nullArgs) {
        return getTable(session, true);
    }

    public void setColumns(ArrayList<Column> columns) {
        this.columns = columns.toArray(new Column[0]);
    }

    private ValueResultSet getTable(Session session, boolean onlyColumnList) {
        int totalColumns = columns.length;
        Expression[] header = new Expression[totalColumns];
        Database db = session.getDatabase();
        for (int i = 0; i < totalColumns; i++) {
            Column c = columns[i];
            ExpressionColumn col = new ExpressionColumn(db, c);
            header[i] = col;
        }
        LocalResult result = db.getResultFactory().create(session, header, totalColumns);
        if (!onlyColumnList && info.type == TABLE_DISTINCT) {
            result.setDistinct();
        }
        if (!onlyColumnList) {
            int len = totalColumns;
            boolean unnest = info.type == UNNEST, addNumber = false;
            if (unnest) {
                len = args.length;
                if (len < totalColumns) {
                    addNumber = true;
                }
            }
            Value[][] list = new Value[len][];
            int rows = 0;
            for (int i = 0; i < len; i++) {
                Value v = args[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    list[i] = new Value[0];
                } else {
                    int type = v.getValueType();
                    if (type != Value.ARRAY && type != Value.ROW) {
                        v = v.convertTo(Value.ARRAY);
                    }
                    Value[] l = ((ValueCollectionBase) v).getList();
                    list[i] = l;
                    rows = Math.max(rows, l.length);
                }
            }
            for (int row = 0; row < rows; row++) {
                Value[] r = new Value[totalColumns];
                for (int j = 0; j < len; j++) {
                    Value[] l = list[j];
                    Value v;
                    if (l.length <= row) {
                        v = ValueNull.INSTANCE;
                    } else {
                        Column c = columns[j];
                        v = l[row];
                        if (!unnest) {
                            v = c.convert(v).convertPrecision(c.getType().getPrecision(), false)
                                    .convertScale(true, c.getType().getScale());
                        }
                    }
                    r[j] = v;
                }
                if (addNumber) {
                    r[len] = ValueInt.get(row + 1);
                }
                result.addRow(r);
            }
        }
        result.done();
        return ValueResultSet.get(result, Integer.MAX_VALUE);
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        return getExpressionColumns(session, getValueForColumnList(session, null).getResult());
    }

    @Override
    public boolean isConstant() {
        for (Expression e : args) {
            if (!e.isConstant()) {
                return false;
            }
        }
        return true;
    }

}
