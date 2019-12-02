/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.util.New;

/**
 * This class represents the statement
 * DROP ALL OBJECTS
 */
public class DropDatabase extends DefineCommand {

    private boolean dropAllObjects;
    private boolean deleteFiles;

    public DropDatabase(Session session) {
        super(session);
    }

    @Override
    public int update() {
        if (dropAllObjects) {
            dropAllObjects();
        }
        if (deleteFiles) {
            session.getDatabase().setDeleteFilesOnDisconnect(true);
        }
        return 0;
    }

    private void dropAllObjects() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        db.lockMeta(session);

        // There can be dependencies between tables e.g. using computed columns,
        // so we might need to loop over them multiple times.
        boolean runLoopAgain;
        do {
            ArrayList<Table> tables = db.getAllTablesAndViews(false);
            ArrayList<Table> toRemove = New.arrayList();
            for (Table t : tables) {
                if (t.getName() != null &&
                        TableType.VIEW == t.getTableType()) {
                    toRemove.add(t);
                }
            }
            for (Table t : tables) {
                if (t.getName() != null &&
                        TableType.TABLE_LINK == t.getTableType()) {
                    toRemove.add(t);
                }
            }
            for (Table t : tables) {
                if (t.getName() != null &&
                        TableType.TABLE == t.getTableType() &&
                        !t.isHidden()) {
                    toRemove.add(t);
                }
            }
            for (Table t : tables) {
                if (t.getName() != null &&
                        TableType.EXTERNAL_TABLE_ENGINE == t.getTableType() &&
                        !t.isHidden()) {
                    toRemove.add(t);
                }
            }
            runLoopAgain = false;
            for (Table t : toRemove) {
                if (t.getName() == null) {
                    // ignore, already dead
                } else if (db.getDependentTable(t, t) == null) {
                    db.removeSchemaObject(session, t);
                } else {
                    runLoopAgain = true;
                }
            }
        } while (runLoopAgain);

        // TODO session-local temp tables are not removed
        for (Schema schema : db.getAllSchemas()) {
            if (schema.canDrop()) {
                db.removeDatabaseObject(session, schema);
            }
        }
        ArrayList<SchemaObject> list = New.arrayList();
        for (SchemaObject obj : db.getAllSchemaObjects(DbObject.SEQUENCE))  {
            // ignore these. the ones we want to drop will get dropped when we
            // drop their associated tables, and we will ignore the problematic
            // ones that belong to session-local temp tables.
            if (!((Sequence) obj).getBelongsToTable()) {
                list.add(obj);
            }
        }
        // maybe constraints and triggers on system tables will be allowed in
        // the future
        list.addAll(db.getAllSchemaObjects(DbObject.CONSTRAINT));
        list.addAll(db.getAllSchemaObjects(DbObject.TRIGGER));
        list.addAll(db.getAllSchemaObjects(DbObject.CONSTANT));
        list.addAll(db.getAllSchemaObjects(DbObject.FUNCTION_ALIAS));
        for (SchemaObject obj : list) {
            if (obj.isHidden()) {
                continue;
            }
            db.removeSchemaObject(session, obj);
        }
        for (User user : db.getAllUsers()) {
            if (user != session.getUser()) {
                db.removeDatabaseObject(session, user);
            }
        }
        for (Role role : db.getAllRoles()) {
            String sql = role.getCreateSQL();
            // the role PUBLIC must not be dropped
            if (sql != null) {
                db.removeDatabaseObject(session, role);
            }
        }
        ArrayList<DbObject> dbObjects = New.arrayList();
        dbObjects.addAll(db.getAllRights());
        dbObjects.addAll(db.getAllAggregates());
        dbObjects.addAll(db.getAllUserDataTypes());
        for (DbObject obj : dbObjects) {
            String sql = obj.getCreateSQL();
            // the role PUBLIC must not be dropped
            if (sql != null) {
                db.removeDatabaseObject(session, obj);
            }
        }
    }

    public void setDropAllObjects(boolean b) {
        this.dropAllObjects = b;
    }

    public void setDeleteFiles(boolean b) {
        this.deleteFiles = b;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_ALL_OBJECTS;
    }

}
