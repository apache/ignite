/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.util.ArrayList;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A referential constraint.
 */
public class ConstraintReferential extends Constraint {

    private IndexColumn[] columns;
    private IndexColumn[] refColumns;
    private ConstraintActionType deleteAction = ConstraintActionType.RESTRICT;
    private ConstraintActionType updateAction = ConstraintActionType.RESTRICT;
    private Table refTable;
    private Index index;
    private Index refIndex;
    private boolean indexOwner;
    private boolean refIndexOwner;
    private String deleteSQL, updateSQL;
    private boolean skipOwnTable;

    public ConstraintReferential(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    @Override
    public Type getConstraintType() {
        return Constraint.Type.REFERENTIAL;
    }

    /**
     * Create the SQL statement of this object so a copy of the table can be
     * made.
     *
     * @param forTable the table to create the object for
     * @param quotedName the name of this object (quoted if necessary)
     * @return the SQL statement
     */
    @Override
    public String getCreateSQLForCopy(Table forTable, String quotedName) {
        return getCreateSQLForCopy(forTable, refTable, quotedName, true);
    }

    /**
     * Create the SQL statement of this object so a copy of the table can be
     * made.
     *
     * @param forTable the table to create the object for
     * @param forRefTable the referenced table
     * @param quotedName the name of this object (quoted if necessary)
     * @param internalIndex add the index name to the statement
     * @return the SQL statement
     */
    public String getCreateSQLForCopy(Table forTable, Table forRefTable,
            String quotedName, boolean internalIndex) {
        StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
        String mainTable = forTable.getSQL();
        buff.append(mainTable).append(" ADD CONSTRAINT ");
        if (forTable.isHidden()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        IndexColumn[] cols = columns;
        IndexColumn[] refCols = refColumns;
        buff.append(" FOREIGN KEY(");
        for (IndexColumn c : cols) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');
        if (internalIndex && indexOwner && forTable == this.table) {
            buff.append(" INDEX ").append(index.getSQL());
        }
        buff.append(" REFERENCES ");
        String quotedRefTable;
        if (this.table == this.refTable) {
            // self-referencing constraints: need to use new table
            quotedRefTable = forTable.getSQL();
        } else {
            quotedRefTable = forRefTable.getSQL();
        }
        buff.append(quotedRefTable).append('(');
        buff.resetCount();
        for (IndexColumn r : refCols) {
            buff.appendExceptFirst(", ");
            buff.append(r.getSQL());
        }
        buff.append(')');
        if (internalIndex && refIndexOwner && forTable == this.table) {
            buff.append(" INDEX ").append(refIndex.getSQL());
        }
        if (deleteAction != ConstraintActionType.RESTRICT) {
            buff.append(" ON DELETE ").append(deleteAction.getSqlName());
        }
        if (updateAction != ConstraintActionType.RESTRICT) {
            buff.append(" ON UPDATE ").append(updateAction.getSqlName());
        }
        return buff.append(" NOCHECK").toString();
    }


    /**
     * Get a short description of the constraint. This includes the constraint
     * name (if set), and the constraint expression.
     *
     * @param searchIndex the index, or null
     * @param check the row, or null
     * @return the description
     */
    private String getShortDescription(Index searchIndex, SearchRow check) {
        StatementBuilder buff = new StatementBuilder(getName());
        buff.append(": ").append(table.getSQL()).append(" FOREIGN KEY(");
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(") REFERENCES ").append(refTable.getSQL()).append('(');
        buff.resetCount();
        for (IndexColumn r : refColumns) {
            buff.appendExceptFirst(", ");
            buff.append(r.getSQL());
        }
        buff.append(')');
        if (searchIndex != null && check != null) {
            buff.append(" (");
            buff.resetCount();
            Column[] cols = searchIndex.getColumns();
            int len = Math.min(columns.length, cols.length);
            for (int i = 0; i < len; i++) {
                int idx = cols[i].getColumnId();
                Value c = check.getValue(idx);
                buff.appendExceptFirst(", ");
                buff.append(c == null ? "" : c.toString());
            }
            buff.append(')');
        }
        return buff.toString();
    }

    @Override
    public String getCreateSQLWithoutIndexes() {
        return getCreateSQLForCopy(table, refTable, getSQL(), false);
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public void setColumns(IndexColumn[] cols) {
        columns = cols;
    }

    public IndexColumn[] getColumns() {
        return columns;
    }

    @Override
    public HashSet<Column> getReferencedColumns(Table table) {
        HashSet<Column> result = new HashSet<>();
        if (table == this.table) {
            for (IndexColumn c : columns) {
                result.add(c.column);
            }
        } else if (table == this.refTable) {
            for (IndexColumn c : refColumns) {
                result.add(c.column);
            }
        }
        return result;
    }

    public void setRefColumns(IndexColumn[] refCols) {
        refColumns = refCols;
    }

    public IndexColumn[] getRefColumns() {
        return refColumns;
    }

    public void setRefTable(Table refTable) {
        this.refTable = refTable;
        if (refTable.isTemporary()) {
            setTemporary(true);
        }
    }

    /**
     * Set the index to use for this constraint.
     *
     * @param index the index
     * @param isOwner true if the index is generated by the system and belongs
     *            to this constraint
     */
    public void setIndex(Index index, boolean isOwner) {
        this.index = index;
        this.indexOwner = isOwner;
    }

    /**
     * Set the index of the referenced table to use for this constraint.
     *
     * @param refIndex the index
     * @param isRefOwner true if the index is generated by the system and
     *            belongs to this constraint
     */
    public void setRefIndex(Index refIndex, boolean isRefOwner) {
        this.refIndex = refIndex;
        this.refIndexOwner = isRefOwner;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        table.removeConstraint(this);
        refTable.removeConstraint(this);
        if (indexOwner) {
            table.removeIndexOrTransferOwnership(session, index);
        }
        if (refIndexOwner) {
            refTable.removeIndexOrTransferOwnership(session, refIndex);
        }
        database.removeMeta(session, getId());
        refTable = null;
        index = null;
        refIndex = null;
        columns = null;
        refColumns = null;
        deleteSQL = null;
        updateSQL = null;
        table = null;
        invalidate();
    }

    @Override
    public void checkRow(Session session, Table t, Row oldRow, Row newRow) {
        if (!database.getReferentialIntegrity()) {
            return;
        }
        if (!table.getCheckForeignKeyConstraints() ||
                !refTable.getCheckForeignKeyConstraints()) {
            return;
        }
        if (t == table) {
            if (!skipOwnTable) {
                checkRowOwnTable(session, oldRow, newRow);
            }
        }
        if (t == refTable) {
            checkRowRefTable(session, oldRow, newRow);
        }
    }

    private void checkRowOwnTable(Session session, Row oldRow, Row newRow) {
        if (newRow == null) {
            return;
        }
        boolean constraintColumnsEqual = oldRow != null;
        for (IndexColumn col : columns) {
            int idx = col.column.getColumnId();
            Value v = newRow.getValue(idx);
            if (v == ValueNull.INSTANCE) {
                // return early if one of the columns is NULL
                return;
            }
            if (constraintColumnsEqual) {
                if (!database.areEqual(v, oldRow.getValue(idx))) {
                    constraintColumnsEqual = false;
                }
            }
        }
        if (constraintColumnsEqual) {
            // return early if the key columns didn't change
            return;
        }
        if (refTable == table) {
            // special case self referencing constraints:
            // check the inserted row first
            boolean self = true;
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = columns[i].column.getColumnId();
                Value v = newRow.getValue(idx);
                Column refCol = refColumns[i].column;
                int refIdx = refCol.getColumnId();
                Value r = newRow.getValue(refIdx);
                if (!database.areEqual(r, v)) {
                    self = false;
                    break;
                }
            }
            if (self) {
                return;
            }
        }
        Row check = refTable.getTemplateRow();
        for (int i = 0, len = columns.length; i < len; i++) {
            int idx = columns[i].column.getColumnId();
            Value v = newRow.getValue(idx);
            Column refCol = refColumns[i].column;
            int refIdx = refCol.getColumnId();
            check.setValue(refIdx, refCol.convert(v));
        }
        if (!existsRow(session, refIndex, check, null)) {
            throw DbException.get(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1,
                    getShortDescription(refIndex, check));
        }
    }

    private boolean existsRow(Session session, Index searchIndex,
            SearchRow check, Row excluding) {
        Table searchTable = searchIndex.getTable();
        searchTable.lock(session, false, false);
        Cursor cursor = searchIndex.find(session, check, check);
        while (cursor.next()) {
            SearchRow found;
            found = cursor.getSearchRow();
            if (excluding != null && found.getKey() == excluding.getKey()) {
                continue;
            }
            Column[] cols = searchIndex.getColumns();
            boolean allEqual = true;
            int len = Math.min(columns.length, cols.length);
            for (int i = 0; i < len; i++) {
                int idx = cols[i].getColumnId();
                Value c = check.getValue(idx);
                Value f = found.getValue(idx);
                if (searchTable.compareTypeSafe(c, f) != 0) {
                    allEqual = false;
                    break;
                }
            }
            if (allEqual) {
                return true;
            }
        }
        return false;
    }

    private boolean isEqual(Row oldRow, Row newRow) {
        return refIndex.compareRows(oldRow, newRow) == 0;
    }

    private void checkRow(Session session, Row oldRow) {
        SearchRow check = table.getTemplateSimpleRow(false);
        for (int i = 0, len = columns.length; i < len; i++) {
            Column refCol = refColumns[i].column;
            int refIdx = refCol.getColumnId();
            Column col = columns[i].column;
            Value v = col.convert(oldRow.getValue(refIdx));
            if (v == ValueNull.INSTANCE) {
                return;
            }
            check.setValue(col.getColumnId(), v);
        }
        // exclude the row only for self-referencing constraints
        Row excluding = (refTable == table) ? oldRow : null;
        if (existsRow(session, index, check, excluding)) {
            throw DbException.get(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1,
                    getShortDescription(index, check));
        }
    }

    private void checkRowRefTable(Session session, Row oldRow, Row newRow) {
        if (oldRow == null) {
            // this is an insert
            return;
        }
        if (newRow != null && isEqual(oldRow, newRow)) {
            // on an update, if both old and new are the same, don't do anything
            return;
        }
        if (newRow == null) {
            // this is a delete
            if (deleteAction == ConstraintActionType.RESTRICT) {
                checkRow(session, oldRow);
            } else {
                int i = deleteAction == ConstraintActionType.CASCADE ? 0 : columns.length;
                Prepared deleteCommand = getDelete(session);
                setWhere(deleteCommand, i, oldRow);
                updateWithSkipCheck(deleteCommand);
            }
        } else {
            // this is an update
            if (updateAction == ConstraintActionType.RESTRICT) {
                checkRow(session, oldRow);
            } else {
                Prepared updateCommand = getUpdate(session);
                if (updateAction == ConstraintActionType.CASCADE) {
                    ArrayList<Parameter> params = updateCommand.getParameters();
                    for (int i = 0, len = columns.length; i < len; i++) {
                        Parameter param = params.get(i);
                        Column refCol = refColumns[i].column;
                        param.setValue(newRow.getValue(refCol.getColumnId()));
                    }
                }
                setWhere(updateCommand, columns.length, oldRow);
                updateWithSkipCheck(updateCommand);
            }
        }
    }

    private void updateWithSkipCheck(Prepared prep) {
        // TODO constraints: maybe delay the update or support delayed checks
        // (until commit)
        try {
            // TODO multithreaded kernel: this works only if nobody else updates
            // this or the ref table at the same time
            skipOwnTable = true;
            prep.update();
        } finally {
            skipOwnTable = false;
        }
    }

    private void setWhere(Prepared command, int pos, Row row) {
        for (int i = 0, len = refColumns.length; i < len; i++) {
            int idx = refColumns[i].column.getColumnId();
            Value v = row.getValue(idx);
            ArrayList<Parameter> params = command.getParameters();
            Parameter param = params.get(pos + i);
            param.setValue(v);
        }
    }

    public ConstraintActionType getDeleteAction() {
        return deleteAction;
    }

    /**
     * Set the action to apply (restrict, cascade,...) on a delete.
     *
     * @param action the action
     */
    public void setDeleteAction(ConstraintActionType action) {
        if (action == deleteAction && deleteSQL == null) {
            return;
        }
        if (deleteAction != ConstraintActionType.RESTRICT) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON DELETE");
        }
        this.deleteAction = action;
        buildDeleteSQL();
    }

    private void buildDeleteSQL() {
        if (deleteAction == ConstraintActionType.RESTRICT) {
            return;
        }
        StatementBuilder buff = new StatementBuilder();
        if (deleteAction == ConstraintActionType.CASCADE) {
            buff.append("DELETE FROM ").append(table.getSQL());
        } else {
            appendUpdate(buff);
        }
        appendWhere(buff);
        deleteSQL = buff.toString();
    }

    private Prepared getUpdate(Session session) {
        return prepare(session, updateSQL, updateAction);
    }

    private Prepared getDelete(Session session) {
        return prepare(session, deleteSQL, deleteAction);
    }

    public ConstraintActionType getUpdateAction() {
        return updateAction;
    }

    /**
     * Set the action to apply (restrict, cascade,...) on an update.
     *
     * @param action the action
     */
    public void setUpdateAction(ConstraintActionType action) {
        if (action == updateAction && updateSQL == null) {
            return;
        }
        if (updateAction != ConstraintActionType.RESTRICT) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON UPDATE");
        }
        this.updateAction = action;
        buildUpdateSQL();
    }

    private void buildUpdateSQL() {
        if (updateAction == ConstraintActionType.RESTRICT) {
            return;
        }
        StatementBuilder buff = new StatementBuilder();
        appendUpdate(buff);
        appendWhere(buff);
        updateSQL = buff.toString();
    }

    @Override
    public void rebuild() {
        buildUpdateSQL();
        buildDeleteSQL();
    }

    private Prepared prepare(Session session, String sql, ConstraintActionType action) {
        Prepared command = session.prepare(sql);
        if (action != ConstraintActionType.CASCADE) {
            ArrayList<Parameter> params = command.getParameters();
            for (int i = 0, len = columns.length; i < len; i++) {
                Column column = columns[i].column;
                Parameter param = params.get(i);
                Value value;
                if (action == ConstraintActionType.SET_NULL) {
                    value = ValueNull.INSTANCE;
                } else {
                    Expression expr = column.getDefaultExpression();
                    if (expr == null) {
                        throw DbException.get(ErrorCode.NO_DEFAULT_SET_1, column.getName());
                    }
                    value = expr.getValue(session);
                }
                param.setValue(value);
            }
        }
        return command;
    }

    private void appendUpdate(StatementBuilder buff) {
        buff.append("UPDATE ").append(table.getSQL()).append(" SET ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" , ");
            buff.append(Parser.quoteIdentifier(c.column.getName())).append("=?");
        }
    }

    private void appendWhere(StatementBuilder buff) {
        buff.append(" WHERE ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append(Parser.quoteIdentifier(c.column.getName())).append("=?");
        }
    }

    @Override
    public Table getRefTable() {
        return refTable;
    }

    @Override
    public boolean usesIndex(Index idx) {
        return idx == index || idx == refIndex;
    }

    @Override
    public void setIndexOwner(Index index) {
        if (this.index == index) {
            indexOwner = true;
        } else if (this.refIndex == index) {
            refIndexOwner = true;
        } else {
            DbException.throwInternalError(index + " " + toString());
        }
    }

    @Override
    public boolean isBefore() {
        return false;
    }

    @Override
    public void checkExistingData(Session session) {
        if (session.getDatabase().isStarting()) {
            // don't check at startup
            return;
        }
        session.startStatementWithinTransaction();
        StatementBuilder buff = new StatementBuilder("SELECT 1 FROM (SELECT ");
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(" FROM ").append(table.getSQL()).append(" WHERE ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append(c.getSQL()).append(" IS NOT NULL ");
        }
        buff.append(" ORDER BY ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(") C WHERE NOT EXISTS(SELECT 1 FROM ").
            append(refTable.getSQL()).append(" P WHERE ");
        buff.resetCount();
        int i = 0;
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append("C.").append(c.getSQL()).append('=').
                append("P.").append(refColumns[i++].getSQL());
        }
        buff.append(')');
        String sql = buff.toString();
        ResultInterface r = session.prepare(sql).query(1);
        if (r.next()) {
            throw DbException.get(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1,
                    getShortDescription(null, null));
        }
    }

    @Override
    public Index getUniqueIndex() {
        return refIndex;
    }

}
