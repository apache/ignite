/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.message.DbException;

/**
 * This class represents the statement
 * CREATE ROLE
 */
public class CreateRole extends DefineCommand {

    private String roleName;
    private boolean ifNotExists;

    public CreateRole(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setRoleName(String name) {
        this.roleName = name;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (db.findUser(roleName) != null) {
            throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, roleName);
        }
        if (db.findRole(roleName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, roleName);
        }
        int id = getObjectId();
        Role role = new Role(db, id, roleName, false);
        db.addDatabaseObject(session, role);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_ROLE;
    }

}
