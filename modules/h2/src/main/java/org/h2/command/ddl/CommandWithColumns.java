/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.util.New;

public abstract class CommandWithColumns extends SchemaCommand {

    private ArrayList<DefineCommand> constraintCommands;

    private IndexColumn[] pkColumns;

    protected CommandWithColumns(Session session, Schema schema) {
        super(session, schema);
    }

    /**
     * Add a column to this table.
     *
     * @param column
     *            the column to add
     */
    public abstract void addColumn(Column column);

    /**
     * Add a constraint statement to this statement. The primary key definition is
     * one possible constraint statement.
     *
     * @param command
     *            the statement to add
     */
    public void addConstraintCommand(DefineCommand command) {
        if (command instanceof CreateIndex) {
            getConstraintCommands().add(command);
        } else {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                getConstraintCommands().add(command);
            }
        }
    }

    /**
     * For the given list of columns, disable "nullable" for those columns that
     * are primary key columns.
     *
     * @param columns the list of columns
     */
    protected void changePrimaryKeysToNotNull(ArrayList<Column> columns) {
        if (pkColumns != null) {
            for (Column c : columns) {
                for (IndexColumn idxCol : pkColumns) {
                    if (c.getName().equals(idxCol.columnName)) {
                        c.setNullable(false);
                    }
                }
            }
        }
    }

    /**
     * Create the constraints.
     */
    protected void createConstraints() {
        if (constraintCommands != null) {
            for (DefineCommand command : constraintCommands) {
                command.setTransactional(transactional);
                command.update();
            }
        }
    }

    /**
     * For the given list of columns, create sequences for auto-increment
     * columns (if needed), and then get the list of all sequences of the
     * columns.
     *
     * @param columns the columns
     * @param temporary whether generated sequences should be temporary
     * @return the list of sequences (may be empty)
     */
    protected ArrayList<Sequence> generateSequences(ArrayList<Column> columns, boolean temporary) {
        ArrayList<Sequence> sequences = New.arrayList();
        if (columns != null) {
            for (Column c : columns) {
                if (c.isAutoIncrement()) {
                    int objId = getObjectId();
                    c.convertAutoIncrementToSequence(session, getSchema(), objId, temporary);
                    if (!Constants.CLUSTERING_DISABLED.equals(session.getDatabase().getCluster())) {
                        throw DbException.getUnsupportedException("CLUSTERING && auto-increment columns");
                    }
                }
                Sequence seq = c.getSequence();
                if (seq != null) {
                    sequences.add(seq);
                }
            }
        }
        return sequences;
    }

    private ArrayList<DefineCommand> getConstraintCommands() {
        if (constraintCommands == null) {
            constraintCommands = New.arrayList();
        }
        return constraintCommands;
    }

    /**
     * Sets the primary key columns, but also check if a primary key with different
     * columns is already defined.
     *
     * @param columns
     *            the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(IndexColumn[] columns) {
        if (pkColumns != null) {
            int len = columns.length;
            if (len != pkColumns.length) {
                throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
            }
            for (int i = 0; i < len; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }

}
