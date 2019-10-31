/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.message.DbException;

/**
 * This class represents the statement
 * DROP ROLE
 */
public class DropRole extends DefineCommand {

    private String roleName;
    private boolean ifExists;

    public DropRole(Session session) {
        super(session);
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (roleName.equals(Constants.PUBLIC_ROLE_NAME)) {
            throw DbException.get(ErrorCode.ROLE_CAN_NOT_BE_DROPPED_1, roleName);
        }
        Role role = db.findRole(roleName);
        if (role == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, roleName);
            }
        } else {
            db.removeDatabaseObject(session, role);
        }
        return 0;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_ROLE;
    }

}
