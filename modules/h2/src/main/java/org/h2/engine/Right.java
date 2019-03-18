/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * An access right. Rights are regular database objects, but have generated
 * names.
 */
public class Right extends DbObjectBase {

    /**
     * The right bit mask that means: selecting from a table is allowed.
     */
    public static final int SELECT = 1;

    /**
     * The right bit mask that means: deleting rows from a table is allowed.
     */
    public static final int DELETE = 2;

    /**
     * The right bit mask that means: inserting rows into a table is allowed.
     */
    public static final int INSERT = 4;

    /**
     * The right bit mask that means: updating data is allowed.
     */
    public static final int UPDATE = 8;

    /**
     * The right bit mask that means: create/alter/drop schema is allowed.
     */
    public static final int ALTER_ANY_SCHEMA = 16;

    /**
     * The right bit mask that means: select, insert, update, delete, and update
     * for this object is allowed.
     */
    public static final int ALL = SELECT | DELETE | INSERT | UPDATE;

    /**
     * To whom the right is granted.
     */
    private RightOwner grantee;

    /**
     * The granted role, or null if a right was granted.
     */
    private Role grantedRole;

    /**
     * The granted right.
     */
    private int grantedRight;

    /**
     * The object. If the right is global, this is null.
     */
    private DbObject grantedObject;

    public Right(Database db, int id, RightOwner grantee, Role grantedRole) {
        initDbObjectBase(db, id, "RIGHT_" + id, Trace.USER);
        this.grantee = grantee;
        this.grantedRole = grantedRole;
    }

    public Right(Database db, int id, RightOwner grantee, int grantedRight,
            DbObject grantedObject) {
        initDbObjectBase(db, id, "" + id, Trace.USER);
        this.grantee = grantee;
        this.grantedRight = grantedRight;
        this.grantedObject = grantedObject;
    }

    private static boolean appendRight(StringBuilder buff, int right, int mask,
            String name, boolean comma) {
        if ((right & mask) != 0) {
            if (comma) {
                buff.append(", ");
            }
            buff.append(name);
            return true;
        }
        return comma;
    }

    public String getRights() {
        StringBuilder buff = new StringBuilder();
        if (grantedRight == ALL) {
            buff.append("ALL");
        } else {
            boolean comma = false;
            comma = appendRight(buff, grantedRight, SELECT, "SELECT", comma);
            comma = appendRight(buff, grantedRight, DELETE, "DELETE", comma);
            comma = appendRight(buff, grantedRight, INSERT, "INSERT", comma);
            comma = appendRight(buff, grantedRight, ALTER_ANY_SCHEMA,
                    "ALTER ANY SCHEMA", comma);
            appendRight(buff, grantedRight, UPDATE, "UPDATE", comma);
        }
        return buff.toString();
    }

    public Role getGrantedRole() {
        return grantedRole;
    }

    public DbObject getGrantedObject() {
        return grantedObject;
    }

    public DbObject getGrantee() {
        return grantee;
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        return getCreateSQLForCopy(table);
    }

    private String getCreateSQLForCopy(DbObject object) {
        StringBuilder buff = new StringBuilder();
        buff.append("GRANT ");
        if (grantedRole != null) {
            buff.append(grantedRole.getSQL());
        } else {
            buff.append(getRights());
            if (object != null) {
                if (object instanceof Schema) {
                    buff.append(" ON SCHEMA ").append(object.getSQL());
                } else if (object instanceof Table) {
                    buff.append(" ON ").append(object.getSQL());
                }
            }
        }
        buff.append(" TO ").append(grantee.getSQL());
        return buff.toString();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQLForCopy(grantedObject);
    }

    @Override
    public int getType() {
        return DbObject.RIGHT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        if (grantedRole != null) {
            grantee.revokeRole(grantedRole);
        } else {
            grantee.revokeRight(grantedObject);
        }
        database.removeMeta(session, getId());
        grantedRole = null;
        grantedObject = null;
        grantee = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    public void setRightMask(int rightMask) {
        grantedRight = rightMask;
    }

    public int getRightMask() {
        return grantedRight;
    }

}
