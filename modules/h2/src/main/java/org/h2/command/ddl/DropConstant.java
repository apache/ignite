/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Constant;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * DROP CONSTANT
 */
public class DropConstant extends SchemaCommand {

    private String constantName;
    private boolean ifExists;

    public DropConstant(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Constant constant = getSchema().findConstant(constantName);
        if (constant == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTANT_NOT_FOUND_1, constantName);
            }
        } else {
            db.removeSchemaObject(session, constant);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_CONSTANT;
    }

}
