/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.util.StatementBuilder;

/**
 * This class represents the statement
 * DROP SCHEMA
 */
public class DropSchema extends DefineCommand {

    private String schemaName;
    private boolean ifExists;
    private ConstraintActionType dropAction;

    public DropSchema(Session session) {
        super(session);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT : ConstraintActionType.CASCADE;
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    @Override
    public int update() {
        session.getUser().checkSchemaAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Schema schema = db.findSchema(schemaName);
        if (schema == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
            }
        } else {
            if (!schema.canDrop()) {
                throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, schemaName);
            }
            if (dropAction == ConstraintActionType.RESTRICT && !schema.isEmpty()) {
                StatementBuilder buff = new StatementBuilder();
                for (SchemaObject object : schema.getAll()) {
                    buff.appendExceptFirst(", ");
                    buff.append(object.getName());
                }
                if (buff.length() > 0) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_2, schemaName, buff.toString());
                }
            }
            db.removeDatabaseObject(session, schema);
        }
        return 0;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_SCHEMA;
    }

}
