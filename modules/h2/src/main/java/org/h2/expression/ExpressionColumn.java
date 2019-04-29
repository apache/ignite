/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.command.dml.SelectListColumnResolver;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.condition.Comparison;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.ExtTypeInfo;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression {

    private final Database database;
    private final String schemaName;
    private final String tableAlias;
    private final String columnName;
    private final boolean rowId;
    private ColumnResolver columnResolver;
    private int queryLevel;
    private Column column;
    private String derivedName;

    public ExpressionColumn(Database database, Column column) {
        this.database = database;
        this.column = column;
        this.schemaName = null;
        this.tableAlias = null;
        this.columnName = null;
        this.rowId = column.isRowId();
    }

    public ExpressionColumn(Database database, String schemaName,
            String tableAlias, String columnName, boolean rowId) {
        this.database = database;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
        this.rowId = rowId;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        if (schemaName != null) {
            Parser.quoteIdentifier(builder, schemaName, alwaysQuote).append('.');
        }
        if (tableAlias != null) {
            Parser.quoteIdentifier(builder, tableAlias, alwaysQuote).append('.');
        }
        if (column != null) {
            if (derivedName != null) {
                Parser.quoteIdentifier(builder, derivedName, alwaysQuote);
            } else {
                column.getSQL(builder, alwaysQuote);
            }
        } else if (rowId) {
            builder.append(columnName);
        } else {
            Parser.quoteIdentifier(builder, columnName, alwaysQuote);
        }
        return builder;
    }

    public TableFilter getTableFilter() {
        return columnResolver == null ? null : columnResolver.getTableFilter();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        if (tableAlias != null && !database.equalsIdentifiers(
                tableAlias, resolver.getTableAlias())) {
            return;
        }
        if (schemaName != null && !database.equalsIdentifiers(
                schemaName, resolver.getSchemaName())) {
            return;
        }
        if (rowId) {
            Column col = resolver.getRowIdColumn();
            if (col != null) {
                mapColumn(resolver, col, level);
            }
            return;
        }
        for (Column col : resolver.getColumns()) {
            String n = resolver.getDerivedColumnName(col);
            boolean derived;
            if (n == null) {
                n = col.getName();
                derived  = false;
            } else {
                derived = true;
            }
            if (database.equalsIdentifiers(columnName, n)) {
                mapColumn(resolver, col, level);
                if (derived) {
                    derivedName = n;
                }
                return;
            }
        }
        Column[] columns = resolver.getSystemColumns();
        for (int i = 0; columns != null && i < columns.length; i++) {
            Column col = columns[i];
            if (database.equalsIdentifiers(columnName, col.getName())) {
                mapColumn(resolver, col, level);
                return;
            }
        }
    }

    private void mapColumn(ColumnResolver resolver, Column col, int level) {
        if (this.columnResolver == null) {
            queryLevel = level;
            column = col;
            this.columnResolver = resolver;
        } else if (queryLevel == level && this.columnResolver != resolver) {
            if (resolver instanceof SelectListColumnResolver) {
                // ignore - already mapped, that's ok
            } else {
                throw DbException.get(ErrorCode.AMBIGUOUS_COLUMN_NAME_1, columnName);
            }
        }
    }

    @Override
    public Expression optimize(Session session) {
        if (columnResolver == null) {
            Schema schema = session.getDatabase().findSchema(
                    tableAlias == null ? session.getCurrentSchemaName() : tableAlias);
            if (schema != null) {
                Constant constant = schema.findConstant(columnName);
                if (constant != null) {
                    return constant.getValue();
                }
            }
            throw getColumnException(ErrorCode.COLUMN_NOT_FOUND_1);
        }
        return columnResolver.optimize(this, column);
    }

    /**
     * Get exception to throw, with column and table info added
     * @param code SQL error code
     * @return DbException
     */
    public DbException getColumnException(int code) {
        String name = columnName;
        if (tableAlias != null) {
            name = tableAlias + '.' + name;
            if (schemaName != null) {
                name = schemaName + '.' + name;
            }
        }
        return DbException.get(code, name);
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        Select select = columnResolver.getSelect();
        if (select == null) {
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL(false));
        }
        SelectGroups groupData = select.getGroupDataIfCurrent(false);
        if (groupData == null) {
            // this is a different level (the enclosing query)
            return;
        }
        Value v = (Value) groupData.getCurrentGroupExprData(this);
        if (v == null) {
            groupData.setCurrentGroupExprData(this, columnResolver.getValue(column));
        } else if (!select.isGroupWindowStage2()) {
            if (!database.areEqual(columnResolver.getValue(column), v)) {
                throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL(false));
            }
        }
    }

    @Override
    public Value getValue(Session session) {
        Select select = columnResolver.getSelect();
        if (select != null) {
            SelectGroups groupData = select.getGroupDataIfCurrent(false);
            if (groupData != null) {
                Value v = (Value) groupData.getCurrentGroupExprData(this);
                if (v != null) {
                    return v;
                }
                if (select.isGroupWindowStage2()) {
                    throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL(false));
                }
            }
        }
        Value value = columnResolver.getValue(column);
        if (value == null) {
            if (select == null) {
                throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, getSQL(false));
            } else {
                throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL(false));
            }
        }
        /*
         * ENUM values are stored as integers.
         */
        if (value != ValueNull.INSTANCE) {
            ExtTypeInfo extTypeInfo = column.getType().getExtTypeInfo();
            if (extTypeInfo != null) {
                return extTypeInfo.cast(value);
            }
        }
        return value;
    }

    @Override
    public TypeInfo getType() {
        return column == null ? TypeInfo.TYPE_UNKNOWN : column.getType();
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
    }

    public Column getColumn() {
        return column;
    }

    public String getOriginalColumnName() {
        return columnName;
    }

    public String getOriginalTableAliasName() {
        return tableAlias;
    }

    @Override
    public String getColumnName() {
        return columnName != null ? columnName : column.getName();
    }

    @Override
    public String getSchemaName() {
        Table table = column.getTable();
        return table == null ? null : table.getSchema().getName();
    }

    @Override
    public String getTableName() {
        Table table = column.getTable();
        return table == null ? null : table.getName();
    }

    @Override
    public String getAlias() {
        if (column != null) {
            if (columnResolver != null) {
                String name = columnResolver.getDerivedColumnName(column);
                if (name != null) {
                    return name;
                }
            }
            return column.getName();
        }
        if (tableAlias != null) {
            return tableAlias + "." + columnName;
        }
        return columnName;
    }

    @Override
    public boolean isAutoIncrement() {
        return column.getSequence() != null;
    }

    @Override
    public int getNullable() {
        return column.isNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
            return false;
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return this.queryLevel < visitor.getQueryLevel();
        case ExpressionVisitor.EVALUATABLE:
            // if this column belongs to a 'higher level' query and is
            // therefore just a parameter
            if (visitor.getQueryLevel() < this.queryLevel) {
                return true;
            }
            if (getTableFilter() == null) {
                return false;
            }
            return getTableFilter().isEvaluatable();
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            visitor.addDataModificationId(column.getTable().getMaxDataModificationId());
            return true;
        case ExpressionVisitor.NOT_FROM_RESOLVER:
            return columnResolver != visitor.getResolver();
        case ExpressionVisitor.GET_DEPENDENCIES:
            if (column != null) {
                visitor.addDependency(column.getTable());
            }
            return true;
        case ExpressionVisitor.GET_COLUMNS1:
            if (column == null) {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, getSQL(false));
            }
            visitor.addColumn1(column);
            return true;
        case ExpressionVisitor.GET_COLUMNS2:
            if (column == null) {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, getSQL(false));
            }
            visitor.addColumn2(column);
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 2;
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        TableFilter tf = getTableFilter();
        if (filter == tf && column.getType().getValueType() == Value.BOOLEAN) {
            IndexCondition cond = IndexCondition.get(
                    Comparison.EQUAL, this, ValueExpression.get(
                            ValueBoolean.TRUE));
            filter.addIndexCondition(cond);
        }
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.FALSE));
    }

}
