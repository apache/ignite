/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.TriggerObject;
import org.h2.table.Table;

/**
 * This class represents the statement
 * DROP TRIGGER
 */
public class DropTrigger extends SchemaCommand {

    private String triggerName;
    private boolean ifExists;

    public DropTrigger(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        TriggerObject trigger = getSchema().findTrigger(triggerName);
        if (trigger == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TRIGGER_NOT_FOUND_1, triggerName);
            }
        } else {
            Table table = trigger.getTable();
            session.getUser().checkRight(table, Right.ALL);
            db.removeSchemaObject(session, trigger);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TRIGGER;
    }

}
