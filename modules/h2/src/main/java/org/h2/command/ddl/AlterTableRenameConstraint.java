/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER TABLE RENAME CONSTRAINT
 */
public class AlterTableRenameConstraint extends SchemaCommand {

    private String constraintName;
    private String newConstraintName;

    public AlterTableRenameConstraint(Session session, Schema schema) {
        super(session, schema);
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }
    public void setNewConstraintName(String newName) {
        this.newConstraintName = newName;
    }

    @Override
    public int update() {
        session.commit(true);
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        if (constraint == null) {
            throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
        }
        if (getSchema().findConstraint(session, newConstraintName) != null ||
                newConstraintName.equals(constraintName)) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1,
                    newConstraintName);
        }
        session.getUser().checkRight(constraint.getTable(), Right.ALL);
        session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
        session.getDatabase().renameSchemaObject(session, constraint, newConstraintName);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_RENAME_CONSTRAINT;
    }

}
