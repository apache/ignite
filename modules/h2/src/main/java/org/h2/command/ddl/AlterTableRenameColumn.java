/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statement
 * ALTER TABLE ALTER COLUMN RENAME
 */
public class AlterTableRenameColumn extends SchemaCommand {

    private boolean ifTableExists;
    private String tableName;
    private String oldName;
    private String newName;

    public AlterTableRenameColumn(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfTableExists(boolean b) {
        this.ifTableExists = b;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setOldColumnName(String oldName) {
        this.oldName = oldName;
    }

    public void setNewColumnName(String newName) {
        this.newName = newName;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        Table table = getSchema().findTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        Column column = table.getColumn(oldName);
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        // we need to update CHECK constraint
        // since it might reference the name of the column
        Expression newCheckExpr = column.getCheckConstraint(session, newName);
        table.renameColumn(column, newName);
        column.removeCheckConstraint();
        column.addCheckConstraint(session, newCheckExpr);
        table.setModified();
        db.updateMeta(session, table);
        for (DbObject child : table.getChildren()) {
            if (child.getCreateSQL() != null) {
                db.updateMeta(session, child);
            }
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_ALTER_COLUMN_RENAME;
    }

}
