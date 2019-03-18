/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import org.h2.table.Table;

/**
 * A database object such as a table, an index, or a user.
 */
public interface DbObject {

    /**
     * The object is of the type table or view.
     */
    int TABLE_OR_VIEW = 0;

    /**
     * This object is an index.
     */
    int INDEX = 1;

    /**
     * This object is a user.
     */
    int USER = 2;

    /**
     * This object is a sequence.
     */
    int SEQUENCE = 3;

    /**
     * This object is a trigger.
     */
    int TRIGGER = 4;

    /**
     * This object is a constraint (check constraint, unique constraint, or
     * referential constraint).
     */
    int CONSTRAINT = 5;

    /**
     * This object is a setting.
     */
    int SETTING = 6;

    /**
     * This object is a role.
     */
    int ROLE = 7;

    /**
     * This object is a right.
     */
    int RIGHT = 8;

    /**
     * This object is an alias for a Java function.
     */
    int FUNCTION_ALIAS = 9;

    /**
     * This object is a schema.
     */
    int SCHEMA = 10;

    /**
     * This object is a constant.
     */
    int CONSTANT = 11;

    /**
     * This object is a user data type (domain).
     */
    int USER_DATATYPE = 12;

    /**
     * This object is a comment.
     */
    int COMMENT = 13;

    /**
     * This object is a user-defined aggregate function.
     */
    int AGGREGATE = 14;

    /**
     * This object is a synonym.
     */
    int SYNONYM = 15;

    /**
     * Get the SQL name of this object (may be quoted).
     *
     * @return the SQL name
     */
    String getSQL();

    /**
     * Get the list of dependent children (for tables, this includes indexes and
     * so on).
     *
     * @return the list of children
     */
    ArrayList<DbObject> getChildren();

    /**
     * Get the database.
     *
     * @return the database
     */
    Database getDatabase();

    /**
     * Get the unique object id.
     *
     * @return the object id
     */
    int getId();

    /**
     * Get the name.
     *
     * @return the name
     */
    String getName();

    /**
     * Build a SQL statement to re-create the object, or to create a copy of the
     * object with a different name or referencing a different table
     *
     * @param table the new table
     * @param quotedName the quoted name
     * @return the SQL statement
     */
    String getCreateSQLForCopy(Table table, String quotedName);

    /**
     * Construct the original CREATE ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    String getCreateSQL();

    /**
     * Construct a DROP ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    String getDropSQL();

    /**
     * Get the object type.
     *
     * @return the object type
     */
    int getType();

    /**
     * Delete all dependent children objects and resources of this object.
     *
     * @param session the session
     */
    void removeChildrenAndResources(Session session);

    /**
     * Check if renaming is allowed. Does nothing when allowed.
     */
    void checkRename();

    /**
     * Rename the object.
     *
     * @param newName the new name
     */
    void rename(String newName);

    /**
     * Check if this object is temporary (for example, a temporary table).
     *
     * @return true if is temporary
     */
    boolean isTemporary();

    /**
     * Tell this object that it is temporary or not.
     *
     * @param temporary the new value
     */
    void setTemporary(boolean temporary);

    /**
     * Change the comment of this object.
     *
     * @param comment the new comment, or null for no comment
     */
    void setComment(String comment);

    /**
     * Get the current comment of this object.
     *
     * @return the comment, or null if not set
     */
    String getComment();

}
