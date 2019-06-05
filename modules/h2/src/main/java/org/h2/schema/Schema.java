/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateSynonymData;
import org.h2.command.ddl.CreateTableData;
import org.h2.constraint.Constraint;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.DbObjectBase;
import org.h2.engine.DbSettings;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.engine.User;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.mvstore.db.MVTableEngine;
import org.h2.table.RegularTable;
import org.h2.table.Table;
import org.h2.table.TableLink;
import org.h2.table.TableSynonym;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * A schema as created by the SQL statement
 * CREATE SCHEMA
 */
public class Schema extends DbObjectBase {

    private User owner;
    private final boolean system;
    private ArrayList<String> tableEngineParams;

    private final ConcurrentHashMap<String, Table> tablesAndViews;
    private final ConcurrentHashMap<String, TableSynonym> synonyms;
    private final ConcurrentHashMap<String, Index> indexes;
    private final ConcurrentHashMap<String, Sequence> sequences;
    private final ConcurrentHashMap<String, TriggerObject> triggers;
    private final ConcurrentHashMap<String, Constraint> constraints;
    private final ConcurrentHashMap<String, Constant> constants;
    private final ConcurrentHashMap<String, FunctionAlias> functions;

    /**
     * The set of returned unique names that are not yet stored. It is used to
     * avoid returning the same unique name twice when multiple threads
     * concurrently create objects.
     */
    private final HashSet<String> temporaryUniqueNames = new HashSet<>();

    /**
     * Create a new schema object.
     *
     * @param database the database
     * @param id the object id
     * @param schemaName the schema name
     * @param owner the owner of the schema
     * @param system if this is a system schema (such a schema can not be
     *            dropped)
     */
    public Schema(Database database, int id, String schemaName, User owner,
            boolean system) {
        tablesAndViews = database.newConcurrentStringMap();
        synonyms = database.newConcurrentStringMap();
        indexes = database.newConcurrentStringMap();
        sequences = database.newConcurrentStringMap();
        triggers = database.newConcurrentStringMap();
        constraints = database.newConcurrentStringMap();
        constants = database.newConcurrentStringMap();
        functions = database.newConcurrentStringMap();
        initDbObjectBase(database, id, schemaName, Trace.SCHEMA);
        this.owner = owner;
        this.system = system;
    }

    /**
     * Check if this schema can be dropped. System schemas can not be dropped.
     *
     * @return true if it can be dropped
     */
    public boolean canDrop() {
        return !system;
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getCreateSQL() {
        if (system) {
            return null;
        }
        return "CREATE SCHEMA IF NOT EXISTS " +
            getSQL() + " AUTHORIZATION " + owner.getSQL();
    }

    @Override
    public int getType() {
        return DbObject.SCHEMA;
    }

    /**
     * Return whether is this schema is empty (does not contain any objects).
     *
     * @return {@code true} if this schema is empty, {@code false} otherwise
     */
    public boolean isEmpty() {
        return tablesAndViews.isEmpty() && synonyms.isEmpty() && indexes.isEmpty() && sequences.isEmpty()
                && triggers.isEmpty() && constraints.isEmpty() && constants.isEmpty() && functions.isEmpty();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        while (triggers != null && triggers.size() > 0) {
            TriggerObject obj = (TriggerObject) triggers.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (constraints != null && constraints.size() > 0) {
            Constraint obj = (Constraint) constraints.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        // There can be dependencies between tables e.g. using computed columns,
        // so we might need to loop over them multiple times.
        boolean runLoopAgain = false;
        do {
            runLoopAgain = false;
            if (tablesAndViews != null) {
                // Loop over a copy because the map is modified underneath us.
                for (Table obj : new ArrayList<>(tablesAndViews.values())) {
                    // Check for null because multiple tables might be deleted
                    // in one go underneath us.
                    if (obj.getName() != null) {
                        if (database.getDependentTable(obj, obj) == null) {
                            database.removeSchemaObject(session, obj);
                        } else {
                            runLoopAgain = true;
                        }
                    }
                }
            }
        } while (runLoopAgain);
        while (indexes != null && indexes.size() > 0) {
            Index obj = (Index) indexes.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (sequences != null && sequences.size() > 0) {
            Sequence obj = (Sequence) sequences.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (constants != null && constants.size() > 0) {
            Constant obj = (Constant) constants.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (functions != null && functions.size() > 0) {
            FunctionAlias obj = (FunctionAlias) functions.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        database.removeMeta(session, getId());
        owner = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the owner of this schema.
     *
     * @return the owner
     */
    public User getOwner() {
        return owner;
    }

    /**
     * Get table engine params of this schema.
     *
     * @return default table engine params
     */
    public ArrayList<String> getTableEngineParams() {
        return tableEngineParams;
    }

    /**
     * Set table engine params of this schema.
     * @param tableEngineParams default table engine params
     */
    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        this.tableEngineParams = tableEngineParams;
    }

    @SuppressWarnings("unchecked")
    private Map<String, SchemaObject> getMap(int type) {
        Map<String, ? extends SchemaObject> result;
        switch (type) {
        case DbObject.TABLE_OR_VIEW:
            result = tablesAndViews;
            break;
        case DbObject.SYNONYM:
            result = synonyms;
            break;
        case DbObject.SEQUENCE:
            result = sequences;
            break;
        case DbObject.INDEX:
            result = indexes;
            break;
        case DbObject.TRIGGER:
            result = triggers;
            break;
        case DbObject.CONSTRAINT:
            result = constraints;
            break;
        case DbObject.CONSTANT:
            result = constants;
            break;
        case DbObject.FUNCTION_ALIAS:
            result = functions;
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        return (Map<String, SchemaObject>) result;
    }

    /**
     * Add an object to this schema.
     * This method must not be called within CreateSchemaObject;
     * use Database.addSchemaObject() instead
     *
     * @param obj the object to add
     */
    public void add(SchemaObject obj) {
        if (SysProperties.CHECK && obj.getSchema() != this) {
            DbException.throwInternalError("wrong schema");
        }
        String name = obj.getName();
        Map<String, SchemaObject> map = getMap(obj.getType());
        if (SysProperties.CHECK && map.get(name) != null) {
            DbException.throwInternalError("object already exists: " + name);
        }
        map.put(name, obj);
        freeUniqueName(name);
    }

    /**
     * Rename an object.
     *
     * @param obj the object to rename
     * @param newName the new name
     */
    public void rename(SchemaObject obj, String newName) {
        int type = obj.getType();
        Map<String, SchemaObject> map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                DbException.throwInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                DbException.throwInternalError("object already exists: " + newName);
            }
        }
        obj.checkRename();
        map.remove(obj.getName());
        freeUniqueName(obj.getName());
        obj.rename(newName);
        map.put(newName, obj);
        freeUniqueName(newName);
    }

    /**
     * Try to find a table or view with this name. This method returns null if
     * no object with this name exists. Local temporary tables are also
     * returned. Synonyms are not returned or resolved.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Table findTableOrView(Session session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null && session != null) {
            table = session.findLocalTempTable(name);
        }
        return table;
    }

    /**
     * Try to find a table or view with this name. This method returns null if
     * no object with this name exists. Local temporary tables are also
     * returned. If a synonym with this name exists, the backing table of the
     * synonym is returned
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Table resolveTableOrView(Session session, String name) {
        Table table = findTableOrView(session, name);
        if (table == null) {
            TableSynonym synonym = synonyms.get(name);
            if (synonym != null) {
                return synonym.getSynonymFor();
            }
            return null;
        }
        return table;
    }

    /**
     * Try to find a synonym with this name. This method returns null if
     * no object with this name exists.
     *
     * @param name the object name
     * @return the object or null
     */
    public TableSynonym getSynonym(String name) {
        return synonyms.get(name);
    }

    /**
     * Try to find an index with this name. This method returns null if
     * no object with this name exists.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Index findIndex(Session session, String name) {
        Index index = indexes.get(name);
        if (index == null) {
            index = session.findLocalTempTableIndex(name);
        }
        return index;
    }

    /**
     * Try to find a trigger with this name. This method returns null if
     * no object with this name exists.
     *
     * @param name the object name
     * @return the object or null
     */
    public TriggerObject findTrigger(String name) {
        return triggers.get(name);
    }

    /**
     * Try to find a sequence with this name. This method returns null if
     * no object with this name exists.
     *
     * @param sequenceName the object name
     * @return the object or null
     */
    public Sequence findSequence(String sequenceName) {
        return sequences.get(sequenceName);
    }

    /**
     * Try to find a constraint with this name. This method returns null if no
     * object with this name exists.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Constraint findConstraint(Session session, String name) {
        Constraint constraint = constraints.get(name);
        if (constraint == null) {
            constraint = session.findLocalTempTableConstraint(name);
        }
        return constraint;
    }

    /**
     * Try to find a user defined constant with this name. This method returns
     * null if no object with this name exists.
     *
     * @param constantName the object name
     * @return the object or null
     */
    public Constant findConstant(String constantName) {
        return constants.get(constantName);
    }

    /**
     * Try to find a user defined function with this name. This method returns
     * null if no object with this name exists.
     *
     * @param functionAlias the object name
     * @return the object or null
     */
    public FunctionAlias findFunction(String functionAlias) {
        return functions.get(functionAlias);
    }

    /**
     * Release a unique object name.
     *
     * @param name the object name
     */
    public void freeUniqueName(String name) {
        if (name != null) {
            synchronized (temporaryUniqueNames) {
                temporaryUniqueNames.remove(name);
            }
        }
    }

    private String getUniqueName(DbObject obj,
            Map<String, ? extends SchemaObject> map, String prefix) {
        String hash = StringUtils.toUpperEnglish(Integer.toHexString(obj.getName().hashCode()));
        String name = null;
        synchronized (temporaryUniqueNames) {
            for (int i = 1, len = hash.length(); i < len; i++) {
                name = prefix + hash.substring(0, i);
                if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                    break;
                }
                name = null;
            }
            if (name == null) {
                prefix = prefix + hash + "_";
                for (int i = 0;; i++) {
                    name = prefix + i;
                    if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                        break;
                    }
                }
            }
            temporaryUniqueNames.add(name);
        }
        return name;
    }

    /**
     * Create a unique constraint name.
     *
     * @param session the session
     * @param table the constraint table
     * @return the unique name
     */
    public String getUniqueConstraintName(Session session, Table table) {
        Map<String, Constraint> tableConstraints;
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            tableConstraints = session.getLocalTempTableConstraints();
        } else {
            tableConstraints = constraints;
        }
        return getUniqueName(table, tableConstraints, "CONSTRAINT_");
    }

    /**
     * Create a unique index name.
     *
     * @param session the session
     * @param table the indexed table
     * @param prefix the index name prefix
     * @return the unique name
     */
    public String getUniqueIndexName(Session session, Table table, String prefix) {
        Map<String, Index> tableIndexes;
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            tableIndexes = session.getLocalTempTableIndexes();
        } else {
            tableIndexes = indexes;
        }
        return getUniqueName(table, tableIndexes, prefix);
    }

    /**
     * Get the table or view with the given name.
     * Local temporary tables are also returned.
     *
     * @param session the session
     * @param name the table or view name
     * @return the table or view
     * @throws DbException if no such object exists
     */
    public Table getTableOrView(Session session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null) {
            if (session != null) {
                table = session.findLocalTempTable(name);
            }
            if (table == null) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, name);
            }
        }
        return table;
    }

    /**
     * Get the index with the given name.
     *
     * @param name the index name
     * @return the index
     * @throws DbException if no such object exists
     */
    public Index getIndex(String name) {
        Index index = indexes.get(name);
        if (index == null) {
            throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, name);
        }
        return index;
    }

    /**
     * Get the constraint with the given name.
     *
     * @param name the constraint name
     * @return the constraint
     * @throws DbException if no such object exists
     */
    public Constraint getConstraint(String name) {
        Constraint constraint = constraints.get(name);
        if (constraint == null) {
            throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, name);
        }
        return constraint;
    }

    /**
     * Get the user defined constant with the given name.
     *
     * @param constantName the constant name
     * @return the constant
     * @throws DbException if no such object exists
     */
    public Constant getConstant(String constantName) {
        Constant constant = constants.get(constantName);
        if (constant == null) {
            throw DbException.get(ErrorCode.CONSTANT_NOT_FOUND_1, constantName);
        }
        return constant;
    }

    /**
     * Get the sequence with the given name.
     *
     * @param sequenceName the sequence name
     * @return the sequence
     * @throws DbException if no such object exists
     */
    public Sequence getSequence(String sequenceName) {
        Sequence sequence = sequences.get(sequenceName);
        if (sequence == null) {
            throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
        }
        return sequence;
    }

    /**
     * Get all objects.
     *
     * @return a (possible empty) list of all objects
     */
    public ArrayList<SchemaObject> getAll() {
        ArrayList<SchemaObject> all = New.arrayList();
        all.addAll(getMap(DbObject.TABLE_OR_VIEW).values());
        all.addAll(getMap(DbObject.SYNONYM).values());
        all.addAll(getMap(DbObject.SEQUENCE).values());
        all.addAll(getMap(DbObject.INDEX).values());
        all.addAll(getMap(DbObject.TRIGGER).values());
        all.addAll(getMap(DbObject.CONSTRAINT).values());
        all.addAll(getMap(DbObject.CONSTANT).values());
        all.addAll(getMap(DbObject.FUNCTION_ALIAS).values());
        return all;
    }

    /**
     * Get all objects of the given type.
     *
     * @param type the object type
     * @return a (possible empty) list of all objects
     */
    public ArrayList<SchemaObject> getAll(int type) {
        Map<String, SchemaObject> map = getMap(type);
        return new ArrayList<>(map.values());
    }

    /**
     * Get all tables and views.
     *
     * @return a (possible empty) list of all objects
     */
    public ArrayList<Table> getAllTablesAndViews() {
        synchronized (database) {
            return new ArrayList<>(tablesAndViews.values());
        }
    }


    public ArrayList<TableSynonym> getAllSynonyms() {
        synchronized (database) {
            return new ArrayList<>(synonyms.values());
        }
    }

    /**
     * Get the table with the given name, if any.
     *
     * @param name the table name
     * @return the table or null if not found
     */
    public Table getTableOrViewByName(String name) {
        synchronized (database) {
            return tablesAndViews.get(name);
        }
    }

    /**
     * Remove an object from this schema.
     *
     * @param obj the object to remove
     */
    public void remove(SchemaObject obj) {
        String objName = obj.getName();
        Map<String, SchemaObject> map = getMap(obj.getType());
        if (SysProperties.CHECK && !map.containsKey(objName)) {
            DbException.throwInternalError("not found: " + objName);
        }
        map.remove(objName);
        freeUniqueName(objName);
    }

    /**
     * Add a table to the schema.
     *
     * @param data the create table information
     * @return the created {@link Table} object
     */
    public Table createTable(CreateTableData data) {
        synchronized (database) {
            if (!data.temporary || data.globalTemporary) {
                database.lockMeta(data.session);
            }
            data.schema = this;
            if (data.tableEngine == null) {
                DbSettings s = database.getSettings();
                if (s.defaultTableEngine != null) {
                    data.tableEngine = s.defaultTableEngine;
                } else if (s.mvStore) {
                    data.tableEngine = MVTableEngine.class.getName();
                }
            }
            if (data.tableEngine != null) {
                if (data.tableEngineParams == null) {
                    data.tableEngineParams = this.tableEngineParams;
                }
                return database.getTableEngine(data.tableEngine).createTable(data);
            }
            return new RegularTable(data);
        }
    }

    /**
     * Add a table synonym to the schema.
     *
     * @param data the create synonym information
     * @return the created {@link TableSynonym} object
     */
    public TableSynonym createSynonym(CreateSynonymData data) {
        synchronized (database) {
            database.lockMeta(data.session);
            data.schema = this;
            return new TableSynonym(data);
        }
    }

    /**
     * Add a linked table to the schema.
     *
     * @param id the object id
     * @param tableName the table name of the alias
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param originalSchema the schema name of the target table
     * @param originalTable the table name of the target table
     * @param emitUpdates if updates should be emitted instead of delete/insert
     * @param force create the object even if the database can not be accessed
     * @return the {@link TableLink} object
     */
    public TableLink createTableLink(int id, String tableName, String driver,
            String url, String user, String password, String originalSchema,
            String originalTable, boolean emitUpdates, boolean force) {
        synchronized (database) {
            return new TableLink(this, id, tableName,
                    driver, url, user, password,
                    originalSchema, originalTable, emitUpdates, force);
        }
    }

}
