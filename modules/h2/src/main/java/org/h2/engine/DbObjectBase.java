/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import org.h2.command.Parser;
import org.h2.message.DbException;
import org.h2.message.Trace;

/**
 * The base class for all database objects.
 */
public abstract class DbObjectBase implements DbObject {

    /**
     * The database.
     */
    protected Database database;

    /**
     * The trace module.
     */
    protected Trace trace;

    /**
     * The comment (if set).
     */
    protected String comment;

    private int id;
    private String objectName;
    private long modificationId;
    private boolean temporary;

    /**
     * Initialize some attributes of this object.
     *
     * @param db the database
     * @param objectId the object id
     * @param name the name
     * @param traceModuleId the trace module id
     */
    protected void initDbObjectBase(Database db, int objectId, String name,
            int traceModuleId) {
        this.database = db;
        this.trace = db.getTrace(traceModuleId);
        this.id = objectId;
        this.objectName = name;
        this.modificationId = db.getModificationMetaId();
    }

    /**
     * Build a SQL statement to re-create this object.
     *
     * @return the SQL statement
     */
    @Override
    public abstract String getCreateSQL();

    /**
     * Build a SQL statement to drop this object.
     *
     * @return the SQL statement
     */
    @Override
    public abstract String getDropSQL();

    /**
     * Remove all dependent objects and free all resources (files, blocks in
     * files) of this object.
     *
     * @param session the session
     */
    @Override
    public abstract void removeChildrenAndResources(Session session);

    /**
     * Check if this object can be renamed. System objects may not be renamed.
     */
    @Override
    public abstract void checkRename();

    /**
     * Tell the object that is was modified.
     */
    public void setModified() {
        this.modificationId = database == null ?
                -1 : database.getNextModificationMetaId();
    }

    public long getModificationId() {
        return modificationId;
    }

    protected void setObjectName(String name) {
        objectName = name;
    }

    @Override
    public String getSQL() {
        return Parser.quoteIdentifier(objectName);
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        return null;
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return objectName;
    }

    /**
     * Set the main attributes to null to make sure the object is no longer
     * used.
     */
    protected void invalidate() {
        if (SysProperties.CHECK && id == -1) {
            throw DbException.throwInternalError();
        }
        setModified();
        id = -1;
        database = null;
        trace = null;
        objectName = null;
    }

    public final boolean isValid() {
        return id != -1;
    }

    @Override
    public void rename(String newName) {
        checkRename();
        objectName = newName;
        setModified();
    }

    @Override
    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    @Override
    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public String toString() {
        return objectName + ":" + id + ":" + super.toString();
    }

}
