/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * This class represents the statement
 * DROP INDEX
 */
public class DropIndex extends SchemaCommand {

    private String indexName;
    private boolean ifExists;

    public DropIndex(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        Index index = getSchema().findIndex(session, indexName);
        if (index == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);
            }
        } else {
            Table table = index.getTable();
            session.getUser().checkRight(index.getTable(), Right.ALL);
            Constraint pkConstraint = null;
            ArrayList<Constraint> constraints = table.getConstraints();
            for (int i = 0; constraints != null && i < constraints.size(); i++) {
                Constraint cons = constraints.get(i);
                if (cons.usesIndex(index)) {
                    // can drop primary key index (for compatibility)
                    if (Constraint.Type.PRIMARY_KEY == cons.getConstraintType()) {
                        pkConstraint = cons;
                    } else {
                        throw DbException.get(
                                ErrorCode.INDEX_BELONGS_TO_CONSTRAINT_2,
                                indexName, cons.getName());
                    }
                }
            }
            index.getTable().setModified();
            if (pkConstraint != null) {
                db.removeSchemaObject(session, pkConstraint);
            } else {
                db.removeSchemaObject(session, index);
            }
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_INDEX;
    }

}
