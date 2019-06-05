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
import org.h2.engine.UserAggregate;
import org.h2.message.DbException;

/**
 * This class represents the statement
 * DROP AGGREGATE
 */
public class DropAggregate extends DefineCommand {

    private String name;
    private boolean ifExists;

    public DropAggregate(Session session) {
        super(session);
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        UserAggregate aggregate = db.findAggregate(name);
        if (aggregate == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.AGGREGATE_NOT_FOUND_1, name);
            }
        } else {
            db.removeDatabaseObject(session, aggregate);
        }
        return 0;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_AGGREGATE;
    }

}
