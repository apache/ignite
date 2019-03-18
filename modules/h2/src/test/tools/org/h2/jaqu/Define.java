/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import org.h2.jaqu.Table.IndexType;

/**
 * This class provides utility methods to define primary keys, indexes, and set
 * the name of the table.
 */
public class Define {

    private static TableDefinition<?> currentTableDefinition;
    private static Table currentTable;

    public static void primaryKey(Object... columns) {
        checkInDefine();
        currentTableDefinition.setPrimaryKey(columns);
    }

    public static void index(Object... columns) {
        checkInDefine();
        currentTableDefinition.addIndex(IndexType.STANDARD, columns);
    }

    public static void uniqueIndex(Object... columns) {
        checkInDefine();
        currentTableDefinition.addIndex(IndexType.UNIQUE, columns);
    }

    public static void hashIndex(Object column) {
        checkInDefine();
        currentTableDefinition
                .addIndex(IndexType.HASH, new Object[] { column });
    }

    public static void uniqueHashIndex(Object column) {
        checkInDefine();
        currentTableDefinition.addIndex(IndexType.UNIQUE_HASH,
                new Object[] { column });
    }

    public static void maxLength(Object column, int length) {
        checkInDefine();
        currentTableDefinition.setMaxLength(column, length);
    }

    public static void tableName(String tableName) {
        currentTableDefinition.setTableName(tableName);
    }

    static synchronized <T> void define(TableDefinition<T> tableDefinition,
            Table table) {
        currentTableDefinition = tableDefinition;
        currentTable = table;
        tableDefinition.mapObject(table);
        table.define();
        currentTable = null;
    }

    private static void checkInDefine() {
        if (currentTable == null) {
            throw new RuntimeException(
                    "This method may only be called "
                            + "from within the define() method, and the define() method "
                            + "is called by the framework.");
        }
    }

}
