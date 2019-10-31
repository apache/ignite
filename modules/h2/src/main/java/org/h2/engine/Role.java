/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;

/**
 * Represents a role. Roles can be granted to users, and to other roles.
 */
public class Role extends RightOwner {

    private final boolean system;

    public Role(Database database, int id, String roleName, boolean system) {
        super(database, id, roleName, Trace.USER);
        this.system = system;
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    /**
     * Get the CREATE SQL statement for this object.
     *
     * @param ifNotExists true if IF NOT EXISTS should be used
     * @return the SQL statement
     */
    public String getCreateSQL(boolean ifNotExists) {
        if (system) {
            return null;
        }
        StringBuilder buff = new StringBuilder("CREATE ROLE ");
        if (ifNotExists) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        return buff.toString();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(false);
    }

    @Override
    public int getType() {
        return DbObject.ROLE;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        for (User user : database.getAllUsers()) {
            Right right = user.getRightForRole(this);
            if (right != null) {
                database.removeDatabaseObject(session, right);
            }
        }
        for (Role r2 : database.getAllRoles()) {
            Right right = r2.getRightForRole(this);
            if (right != null) {
                database.removeDatabaseObject(session, right);
            }
        }
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

}
