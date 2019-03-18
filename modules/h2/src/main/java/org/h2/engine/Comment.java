/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.StringUtils;

/**
 * Represents a database object comment.
 */
public class Comment extends DbObjectBase {

    private final int objectType;
    private final String objectName;
    private String commentText;

    public Comment(Database database, int id, DbObject obj) {
        initDbObjectBase(database, id,  getKey(obj), Trace.DATABASE);
        this.objectType = obj.getType();
        this.objectName = obj.getSQL();
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    private static String getTypeName(int type) {
        switch (type) {
        case DbObject.CONSTANT:
            return "CONSTANT";
        case DbObject.CONSTRAINT:
            return "CONSTRAINT";
        case DbObject.FUNCTION_ALIAS:
            return "ALIAS";
        case DbObject.INDEX:
            return "INDEX";
        case DbObject.ROLE:
            return "ROLE";
        case DbObject.SCHEMA:
            return "SCHEMA";
        case DbObject.SEQUENCE:
            return "SEQUENCE";
        case DbObject.TABLE_OR_VIEW:
            return "TABLE";
        case DbObject.TRIGGER:
            return "TRIGGER";
        case DbObject.USER:
            return "USER";
        case DbObject.USER_DATATYPE:
            return "DOMAIN";
        default:
            // not supported by parser, but required when trying to find a
            // comment
            return "type" + type;
        }
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("COMMENT ON ");
        buff.append(getTypeName(objectType)).append(' ').
                append(objectName).append(" IS ");
        if (commentText == null) {
            buff.append("NULL");
        } else {
            buff.append(StringUtils.quoteStringSQL(commentText));
        }
        return buff.toString();
    }

    @Override
    public int getType() {
        return DbObject.COMMENT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    /**
     * Get the comment key name for the given database object. This key name is
     * used internally to associate the comment to the object.
     *
     * @param obj the object
     * @return the key name
     */
    static String getKey(DbObject obj) {
        return getTypeName(obj.getType()) + " " + obj.getSQL();
    }

    /**
     * Set the comment text.
     *
     * @param comment the text
     */
    public void setCommentText(String comment) {
        this.commentText = comment;
    }

}
