/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import java.io.File;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import org.h2.command.Prepared;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import android.content.ContentValues;
import android.database.Cursor;

/**
 * This class represents a database connection.
 */
@SuppressWarnings("unused")
public class H2Database {

    /**
     * When a conflict occurs, abort the current statement, but don't roll back
     * the transaction. This is the default value.
     */
    public static final int CONFLICT_ABORT = 2;

    /**
     * When a conflict occurs, return SQLITE_CONSTRAINT, but don't roll back the
     * transaction.
     */
    public static final int CONFLICT_FAIL = 3;

    /**
     * When a conflict occurs, continue, but don't modify the conflicting row.
     */
    public static final int CONFLICT_IGNORE = 4;

    /**
     * When a conflict occurs, do nothing.
     */
    public static final int CONFLICT_NONE = 0;

    /**
     * When a conflict occurs, the existing rows are replaced.
     */
    public static final int CONFLICT_REPLACE = 5;

    /**
     * When a conflict occurs, the transaction is rolled back.
     */
    public static final int CONFLICT_ROLLBACK = 1;

    /**
     * Create a new database if it doesn't exist.
     */
    public static final int CREATE_IF_NECESSARY = 0x10000000;

    /**
     * This flag has no effect.
     */
    public static final int NO_LOCALIZED_COLLATORS = 0x10;

    /**
     * Open the database in read-only mode.
     */
    public static final int OPEN_READONLY = 1;

    /**
     * Open the database in read-write mode (default).
     */
    public static final int OPEN_READWRITE = 0;

    private final Session session;
    private final CursorFactory factory;

    H2Database(Session session, CursorFactory factory) {
        this.factory = factory;
        this.session = session;
    }

    /**
     * Create a new in-memory database.
     *
     * @param factory the cursor factory
     * @return a connection to this database
     */
    public static H2Database create(H2Database.CursorFactory factory) {
        ConnectionInfo ci = new ConnectionInfo("mem:");
        Database db = new Database(ci, null);
        Session s = db.getSystemSession();
        return new H2Database(s, factory);
    }

    /**
     * Open a connection to the given database.
     *
     * @param path the database file name
     * @param factory the cursor factory
     * @param flags 0, or a combination of OPEN_READONLY and CREATE_IF_NECESSARY
     * @return a connection to this database
     */
    public static H2Database openDatabase(String path,
            H2Database.CursorFactory factory, int flags) {
        ConnectionInfo ci = new ConnectionInfo(path);
        if ((flags & OPEN_READWRITE) != 0) {
            // TODO readonly connections
        }
        if ((flags & CREATE_IF_NECESSARY) == 0) {
            ci.setProperty("IFEXISTS", "TRUE");
        }
        ci.setProperty("FILE_LOCK", "FS");
        Database db = new Database(ci, null);
        Session s = db.getSystemSession();
        return new H2Database(s, factory);
    }

    /**
     * Open a connection to the given database. The database is created if it
     * doesn't exist yet.
     *
     * @param file the database file
     * @param factory the cursor factory
     * @return a connection to this database
     */
    public static H2Database openOrCreateDatabase(File file,
            H2Database.CursorFactory factory) {
        return openDatabase(file.getPath(), factory, CREATE_IF_NECESSARY);
    }

    /**
     * Open a connection to the given database. The database is created if it
     * doesn't exist yet.
     *
     * @param path the database file name
     * @param factory the cursor factory
     * @return a connection to this database
     */
    public static H2Database openOrCreateDatabase(String path,
            H2Database.CursorFactory factory) {
        return openDatabase(path, factory, CREATE_IF_NECESSARY);
    }

    /**
     * Start a transaction.
     */
    public void beginTransaction() {
        session.setAutoCommit(false);
    }

    /**
     * Start a transaction.
     *
     * @param transactionListener the transaction listener to use
     */
    public void beginTransactionWithListener(
            H2TransactionListener transactionListener) {
        // TODO H2TransactionListener
        session.setAutoCommit(false);
    }

    /**
     * Close the connection.
     */
    public void close() {
        session.close();
    }

    /**
     * Prepare a statement.
     *
     * @param sql the statement
     * @return the prepared statement
     */
    public H2Statement compileStatement(String sql) {
        return new H2Statement(session.prepare(sql));
    }

    /**
     * Delete a number of rows in this database.
     *
     * @param table the table
     * @param whereClause the condition
     * @param whereArgs the parameter values
     * @return the number of rows deleted
     */
    public int delete(String table, String whereClause, String[] whereArgs) {
        return 0;
    }

    /**
     * End the transaction.
     */
    public void endTransaction() {
        // TODO
    }

    /**
     * Execute the given statement.
     *
     * @param sql the statement
     * @param bindArgs the parameter values
     */
    public void execSQL(String sql, Object[] bindArgs) {
        prepare(sql, bindArgs).update();
    }

    /**
     * Execute the given statement.
     *
     * @param sql the statement
     */
    public void execSQL(String sql) {
        session.prepare(sql).update();
    }

    /**
     * TODO
     *
     * @param tables the list of tables
     * @return TODO
     */
    public static String findEditTable(String tables) {
        // TODO
        return null;
    }

    /**
     * Get the maximum size of the database file in bytes.
     *
     * @return the maximum size
     */
    public long getMaximumSize() {
        return Long.MAX_VALUE;
    }

    /**
     * Get the page size of the database in bytes.
     *
     * @return the page size
     */
    public long getPageSize() {
        return 0;
    }

    /**
     * Get the name of the database file.
     *
     * @return the database file name
     */
    public String getPath() {
        return null;
    }

    /**
     * TODO
     *
     * @return TODO
     */
    public Map<String, String> getSyncedTables() {
        return null;
    }

    /**
     * Get the database version.
     *
     * @return the database version
     */
    public int getVersion() {
        return 0;
    }

    /**
     * Check if there is an open transaction.
     *
     * @return true if there is
     */
    public boolean inTransaction() {
        return false;
    }

    /**
     * Insert a row.
     *
     * @param table the table
     * @param nullColumnHack not used
     * @param values the values
     * @return TODO
     */
    public long insert(String table, String nullColumnHack, ContentValues values) {
        return 0;
    }

    /**
     * Try to insert a row.
     *
     * @param table the table
     * @param nullColumnHack not used
     * @param values the values
     * @return TODO
     */
    public long insertOrThrow(String table, String nullColumnHack,
            ContentValues values) {
        return 0;
    }

    /**
     * Try to insert a row, using the given conflict resolution option.
     *
     * @param table the table
     * @param nullColumnHack not used
     * @param initialValues the values
     * @param conflictAlgorithm what conflict resolution to use
     * @return TODO
     */
    public long insertWithOnConflict(String table, String nullColumnHack,
            ContentValues initialValues, int conflictAlgorithm) {
        return 0;
    }

    /**
     * Check if the database is locked by the current thread.
     *
     * @return true if it is
     */
    public boolean isDbLockedByCurrentThread() {
        return false;
    }

    /**
     * Check if the database is locked by a different thread.
     *
     * @return true if it is
     */
    public boolean isDbLockedByOtherThreads() {
        return false;
    }

    /**
     * Check if the connection is open.
     *
     * @return true if it is
     */
    public boolean isOpen() {
        return false;
    }

    /**
     * Check if the connection is read-only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return false;
    }

    /**
     * TODO
     *
     * @param table the table
     * @param deletedTable TODO
     */
    public void markTableSyncable(String table, String deletedTable) {
        // TODO
    }

    /**
     * TODO
     *
     * @param table the table
     * @param foreignKey the foreign key
     * @param updateTable TODO
     */
    public void markTableSyncable(String table, String foreignKey,
            String updateTable) {
        // TODO
    }

    /**
     * Check if an upgrade is required.
     *
     * @param newVersion the new version
     * @return true if the current version doesn't match
     */
    public boolean needUpgrade(int newVersion) {
        return false;
    }

    /**
     * Execute the SELECT statement for the given parameters.
     *
     * @param distinct if only distinct rows should be returned
     * @param table the table
     * @param columns the list of columns
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the cursor
     */
    public Cursor query(boolean distinct, String table, String[] columns,
            String selection, String[] selectionArgs, String groupBy,
            String having, String orderBy, String limit) {
        return null;
    }

    /**
     * Execute the SELECT statement for the given parameters.
     *
     * @param table the table
     * @param columns the list of columns
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @return the cursor
     */
    public Cursor query(String table, String[] columns, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy) {
        return null;
    }

    /**
     * Execute the SELECT statement for the given parameters.
     *
     * @param table the table
     * @param columns the list of columns
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the cursor
     */
    public Cursor query(String table, String[] columns, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy, String limit) {
        return null;
    }

    /**
     * Execute the SELECT statement for the given parameters.
     *
     * @param cursorFactory the cursor factory to use
     * @param distinct if only distinct rows should be returned
     * @param table the table
     * @param columns the list of columns
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the cursor
     */
    public Cursor queryWithFactory(H2Database.CursorFactory cursorFactory,
            boolean distinct, String table, String[] columns, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy, String limit) {
        return null;
    }

    /**
     * Execute the query.
     *
     * @param sql the SQL statement
     * @param selectionArgs the parameter values
     * @return the cursor
     */
    public Cursor rawQuery(String sql, String[] selectionArgs) {
        Prepared prep = prepare(sql, selectionArgs);
        ResultInterface result = prep.query(0);
        return new H2Cursor(result);
    }

    /**
     * Execute the query using the given cursor factory.
     *
     * @param cursorFactory the cursor factory
     * @param sql the SQL statement
     * @param selectionArgs the parameter values
     * @param editTable TODO
     * @return the cursor
     */
    public Cursor rawQueryWithFactory(H2Database.CursorFactory cursorFactory,
            String sql, String[] selectionArgs, String editTable) {
        return null;
    }

    /**
     * Try to release memory.
     *
     * @return TODO
     */
    public static int releaseMemory() {
        return 0;
    }

    /**
     * Replace an existing row in the database.
     *
     * @param table the table
     * @param nullColumnHack ignored
     * @param initialValues the values
     * @return TODO
     */
    public long replace(String table, String nullColumnHack,
            ContentValues initialValues) {
        return 0;
    }

    /**
     * Try to replace an existing row in the database.
     *
     * @param table the table
     * @param nullColumnHack ignored
     * @param initialValues the values
     * @return TODO
     */
    public long replaceOrThrow(String table, String nullColumnHack,
            ContentValues initialValues) {
        return 0;
    }

    /**
     * Set the locale.
     *
     * @param locale the new locale
     */
    public void setLocale(Locale locale) {
        // TODO
    }

    /**
     * Enable or disable thread safety.
     *
     * @param lockingEnabled the new value
     */
    public void setLockingEnabled(boolean lockingEnabled) {
        // TODO
    }

    /**
     * Set the maximum database file size.
     *
     * @param numBytes the file size in bytes
     * @return the effective maximum size
     */
    public long setMaximumSize(long numBytes) {
        return 0;
    }

    /**
     * Set the database page size. The value can not be changed once the
     * database exists.
     *
     * @param numBytes the page size
     */
    public void setPageSize(long numBytes) {
        // TODO
    }

    /**
     * Mark the transaction as completed successfully.
     */
    public void setTransactionSuccessful() {
        // TODO
    }

    /**
     * Update the database version.
     *
     * @param version the version
     */
    public void setVersion(int version) {
        // TODO
    }

    /**
     * Update one or multiple rows.
     *
     * @param table the table
     * @param values the values
     * @param whereClause the where condition
     * @param whereArgs the parameter values
     * @return the number of rows updated
     */
    public int update(String table, ContentValues values, String whereClause,
            String[] whereArgs) {
        return 0;
    }

    /**
     * Update one or multiple rows.
     *
     * @param table the table
     * @param values the values
     * @param whereClause the where condition
     * @param whereArgs the parameter values
     * @param conflictAlgorithm the conflict resolution option
     * @return the number of rows updated
     */
    public int updateWithOnConflict(String table, ContentValues values,
            String whereClause, String[] whereArgs, int conflictAlgorithm) {
        return 0;
    }

    /**
     * TODO
     *
     * @deprecated deprecated in API Level 3, use yieldIfContendedSafely
     * @return TODO
     */
    @Deprecated
    public boolean yieldIfContended() {
        return false;
    }

    /**
     * Temporarily pause the transaction.
     *
     * @param sleepAfterYieldDelay TODO
     * @return TODO
     */
    public boolean yieldIfContendedSafely(long sleepAfterYieldDelay) {
        return false;
    }

    /**
     * Temporarily pause the transaction.
     *
     * @return TODO
     */
    public boolean yieldIfContendedSafely() {
        return false;
    }

    /**
     * The cursor factory.
     */
    public interface CursorFactory {

        /**
         * Create a new cursor.
         *
         * @param db the connection
         * @param masterQuery TODO
         * @param editTable TODO
         * @param query TODO
         * @return the cursor
         */
        Cursor newCursor(H2Database db, H2CursorDriver masterQuery,
                String editTable, H2Query query);
    }

    private Prepared prepare(String sql, Object[] args) {
        Prepared prep = session.prepare(sql);
        int len = args.length;
        if (len > 0) {
            ArrayList<Parameter> params = prep.getParameters();
            for (int i = 0; i < len; i++) {
                Parameter p = params.get(i);
                p.setValue(getValue(args[i]));
            }
        }
        return prep;
    }

    private static Value getValue(Object o) {
        if (o == null) {
            return ValueNull.INSTANCE;
        } else if (o instanceof String) {
            return ValueString.get((String) o);
        } else if (o instanceof Integer) {
            return ValueInt.get((Integer) o);
        } else if (o instanceof Long) {
            return ValueLong.get((Integer) o);
        }
        return ValueString.get(o.toString());
        // TODO
    }

    /**
     * Create a new RuntimeException that says this feature is not supported.
     *
     * @return the runtime exception
     */
    public static RuntimeException unsupported() {
        // TODO
        return new RuntimeException("Feature not supported");
    }

}
