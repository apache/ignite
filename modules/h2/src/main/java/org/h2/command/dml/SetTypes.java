/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;

/**
 * The list of setting for a SET statement.
 */
public class SetTypes {

    /**
     * The type of a SET IGNORECASE statement.
     */
    public static final int IGNORECASE = 1;

    /**
     * The type of a SET MAX_LOG_SIZE statement.
     */
    public static final int MAX_LOG_SIZE = 2;

    /**
     * The type of a SET MODE statement.
     */
    public static final int MODE = 3;

    /**
     * The type of a SET READONLY statement.
     */
    public static final int READONLY = 4;

    /**
     * The type of a SET LOCK_TIMEOUT statement.
     */
    public static final int LOCK_TIMEOUT = 5;

    /**
     * The type of a SET DEFAULT_LOCK_TIMEOUT statement.
     */
    public static final int DEFAULT_LOCK_TIMEOUT = 6;

    /**
     * The type of a SET DEFAULT_TABLE_TYPE statement.
     */
    public static final int DEFAULT_TABLE_TYPE = 7;

    /**
     * The type of a SET CACHE_SIZE statement.
     */
    public static final int CACHE_SIZE = 8;

    /**
     * The type of a SET TRACE_LEVEL_SYSTEM_OUT statement.
     */
    public static final int TRACE_LEVEL_SYSTEM_OUT = 9;

    /**
     * The type of a SET TRACE_LEVEL_FILE statement.
     */
    public static final int TRACE_LEVEL_FILE = 10;

    /**
     * The type of a SET TRACE_MAX_FILE_SIZE statement.
     */
    public static final int TRACE_MAX_FILE_SIZE = 11;

    /**
     * The type of a SET COLLATION  statement.
     */
    public static final int COLLATION = 12;

    /**
     * The type of a SET CLUSTER statement.
     */
    public static final int CLUSTER = 13;

    /**
     * The type of a SET WRITE_DELAY statement.
     */
    public static final int WRITE_DELAY = 14;

    /**
     * The type of a SET DATABASE_EVENT_LISTENER statement.
     */
    public static final int DATABASE_EVENT_LISTENER = 15;

    /**
     * The type of a SET MAX_MEMORY_ROWS statement.
     */
    public static final int MAX_MEMORY_ROWS = 16;

    /**
     * The type of a SET LOCK_MODE statement.
     */
    public static final int LOCK_MODE = 17;

    /**
     * The type of a SET DB_CLOSE_DELAY statement.
     */
    public static final int DB_CLOSE_DELAY = 18;

    /**
     * The type of a SET LOG statement.
     */
    public static final int LOG = 19;

    /**
     * The type of a SET THROTTLE statement.
     */
    public static final int THROTTLE = 20;

    /**
     * The type of a SET MAX_MEMORY_UNDO statement.
     */
    public static final int MAX_MEMORY_UNDO = 21;

    /**
     * The type of a SET MAX_LENGTH_INPLACE_LOB statement.
     */
    public static final int MAX_LENGTH_INPLACE_LOB = 22;

    /**
     * The type of a SET COMPRESS_LOB statement.
     */
    public static final int COMPRESS_LOB = 23;

    /**
     * The type of a SET ALLOW_LITERALS statement.
     */
    public static final int ALLOW_LITERALS = 24;

    /**
     * The type of a SET MULTI_THREADED statement.
     */
    public static final int MULTI_THREADED = 25;

    /**
     * The type of a SET SCHEMA statement.
     */
    public static final int SCHEMA = 26;

    /**
     * The type of a SET OPTIMIZE_REUSE_RESULTS statement.
     */
    public static final int OPTIMIZE_REUSE_RESULTS = 27;

    /**
     * The type of a SET SCHEMA_SEARCH_PATH statement.
     */
    public static final int SCHEMA_SEARCH_PATH = 28;

    /**
     * The type of a SET UNDO_LOG statement.
     */
    public static final int UNDO_LOG = 29;

    /**
     * The type of a SET REFERENTIAL_INTEGRITY statement.
     */
    public static final int REFERENTIAL_INTEGRITY = 30;

    /**
     * The type of a SET MVCC statement.
     */
    public static final int MVCC = 31;

    /**
     * The type of a SET MAX_OPERATION_MEMORY statement.
     */
    public static final int MAX_OPERATION_MEMORY = 32;

    /**
     * The type of a SET EXCLUSIVE statement.
     */
    public static final int EXCLUSIVE = 33;

    /**
     * The type of a SET CREATE_BUILD statement.
     */
    public static final int CREATE_BUILD = 34;

    /**
     * The type of a SET \@VARIABLE statement.
     */
    public static final int VARIABLE = 35;

    /**
     * The type of a SET QUERY_TIMEOUT statement.
     */
    public static final int QUERY_TIMEOUT = 36;

    /**
     * The type of a SET REDO_LOG_BINARY statement.
     */
    public static final int REDO_LOG_BINARY = 37;

    /**
     * The type of a SET BINARY_COLLATION statement.
     */
    public static final int BINARY_COLLATION = 38;

    /**
     * The type of a SET JAVA_OBJECT_SERIALIZER statement.
     */
    public static final int JAVA_OBJECT_SERIALIZER = 39;

    /**
     * The type of a SET RETENTION_TIME statement.
     */
    public static final int RETENTION_TIME = 40;

    /**
     * The type of a SET QUERY_STATISTICS statement.
     */
    public static final int QUERY_STATISTICS = 41;

    /**
     * The type of a SET QUERY_STATISTICS_MAX_ENTRIES statement.
     */
    public static final int QUERY_STATISTICS_MAX_ENTRIES = 42;

    /**
     * The type of a SET ROW_FACTORY statement.
     */
    public static final int ROW_FACTORY = 43;

    /**
     * The type of SET BATCH_JOINS statement.
     */
    public static final int BATCH_JOINS = 44;

    /**
     * The type of SET FORCE_JOIN_ORDER statement.
     */
    public static final int FORCE_JOIN_ORDER = 45;

    /**
     * The type of SET LAZY_QUERY_EXECUTION statement.
     */
    public static final int LAZY_QUERY_EXECUTION = 46;

    /**
     * The type of SET BUILTIN_ALIAS_OVERRIDE statement.
     */
    public static final int BUILTIN_ALIAS_OVERRIDE = 47;

    /**
     * The type of a SET COLUMN_NAME_RULES statement.
     */
    public static final int COLUMN_NAME_RULES = 48;

    private static final int COUNT = COLUMN_NAME_RULES + 1;

    private static final ArrayList<String> TYPES;

    private SetTypes() {
        // utility class
    }

    static {
        ArrayList<String> list = new ArrayList<>(COUNT);
        list.add(null);
        list.add(IGNORECASE, "IGNORECASE");
        list.add(MAX_LOG_SIZE, "MAX_LOG_SIZE");
        list.add(MODE, "MODE");
        list.add(READONLY, "READONLY");
        list.add(LOCK_TIMEOUT, "LOCK_TIMEOUT");
        list.add(DEFAULT_LOCK_TIMEOUT, "DEFAULT_LOCK_TIMEOUT");
        list.add(DEFAULT_TABLE_TYPE, "DEFAULT_TABLE_TYPE");
        list.add(CACHE_SIZE, "CACHE_SIZE");
        list.add(TRACE_LEVEL_SYSTEM_OUT, "TRACE_LEVEL_SYSTEM_OUT");
        list.add(TRACE_LEVEL_FILE, "TRACE_LEVEL_FILE");
        list.add(TRACE_MAX_FILE_SIZE, "TRACE_MAX_FILE_SIZE");
        list.add(COLLATION, "COLLATION");
        list.add(CLUSTER, "CLUSTER");
        list.add(WRITE_DELAY, "WRITE_DELAY");
        list.add(DATABASE_EVENT_LISTENER, "DATABASE_EVENT_LISTENER");
        list.add(MAX_MEMORY_ROWS, "MAX_MEMORY_ROWS");
        list.add(LOCK_MODE, "LOCK_MODE");
        list.add(DB_CLOSE_DELAY, "DB_CLOSE_DELAY");
        list.add(LOG, "LOG");
        list.add(THROTTLE, "THROTTLE");
        list.add(MAX_MEMORY_UNDO, "MAX_MEMORY_UNDO");
        list.add(MAX_LENGTH_INPLACE_LOB, "MAX_LENGTH_INPLACE_LOB");
        list.add(COMPRESS_LOB, "COMPRESS_LOB");
        list.add(ALLOW_LITERALS, "ALLOW_LITERALS");
        list.add(MULTI_THREADED, "MULTI_THREADED");
        list.add(SCHEMA, "SCHEMA");
        list.add(OPTIMIZE_REUSE_RESULTS, "OPTIMIZE_REUSE_RESULTS");
        list.add(SCHEMA_SEARCH_PATH, "SCHEMA_SEARCH_PATH");
        list.add(UNDO_LOG, "UNDO_LOG");
        list.add(REFERENTIAL_INTEGRITY, "REFERENTIAL_INTEGRITY");
        list.add(MVCC, "MVCC");
        list.add(MAX_OPERATION_MEMORY, "MAX_OPERATION_MEMORY");
        list.add(EXCLUSIVE, "EXCLUSIVE");
        list.add(CREATE_BUILD, "CREATE_BUILD");
        list.add(VARIABLE, "@");
        list.add(QUERY_TIMEOUT, "QUERY_TIMEOUT");
        list.add(REDO_LOG_BINARY, "REDO_LOG_BINARY");
        list.add(BINARY_COLLATION, "BINARY_COLLATION");
        list.add(JAVA_OBJECT_SERIALIZER, "JAVA_OBJECT_SERIALIZER");
        list.add(RETENTION_TIME, "RETENTION_TIME");
        list.add(QUERY_STATISTICS, "QUERY_STATISTICS");
        list.add(QUERY_STATISTICS_MAX_ENTRIES, "QUERY_STATISTICS_MAX_ENTRIES");
        list.add(ROW_FACTORY, "ROW_FACTORY");
        list.add(BATCH_JOINS, "BATCH_JOINS");
        list.add(FORCE_JOIN_ORDER, "FORCE_JOIN_ORDER");
        list.add(LAZY_QUERY_EXECUTION, "LAZY_QUERY_EXECUTION");
        list.add(BUILTIN_ALIAS_OVERRIDE, "BUILTIN_ALIAS_OVERRIDE");
        list.add(COLUMN_NAME_RULES, "COLUMN_NAME_RULES");
        TYPES = list;
    }

    /**
     * Get the set type number.
     *
     * @param name the set type name
     * @return the number
     */
    public static int getType(String name) {
        return TYPES.indexOf(name);
    }

    public static ArrayList<String> getTypes() {
        return TYPES;
    }

    /**
     * Get the set type name.
     *
     * @param type the type number
     * @return the name
     */
    public static String getTypeName(int type) {
        return TYPES.get(type);
    }

}
