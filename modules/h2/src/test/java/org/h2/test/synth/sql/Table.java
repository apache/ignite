/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.util.ArrayList;
import org.h2.util.New;

/**
 * Represents a table.
 */
class Table {
    private final TestSynth config;
    private String name;
    private boolean temporary;
    private boolean globalTemporary;
    private Column[] columns;
    private Column[] primaryKeys;
    private final ArrayList<Index> indexes = New.arrayList();

    Table(TestSynth config) {
        this.config = config;
    }

    /**
     * Create a new random table.
     *
     * @param config the configuration
     * @return the table
     */
    static Table newRandomTable(TestSynth config) {
        Table table = new Table(config);
        table.name = "T_" + config.randomIdentifier();

        // there is a difference between local temp tables for persistent and
        // in-memory mode
        // table.temporary = config.random().getBoolean(10);
        // if(table.temporary) {
        // if(config.getMode() == TestSynth.H2_MEM) {
        // table.globalTemporary = false;
        // } else {
        // table.globalTemporary = config.random().getBoolean(50);
        // }
        // }

        int len = config.random().getLog(10) + 1;
        table.columns = new Column[len];
        for (int i = 0; i < len; i++) {
            Column col = Column.getRandomColumn(config);
            table.columns[i] = col;
        }
        if (config.random().getBoolean(90)) {
            int pkLen = config.random().getLog(len);
            table.primaryKeys = new Column[pkLen];
            for (int i = 0; i < pkLen; i++) {
                Column pk = null;
                do {
                    pk = table.columns[config.random().getInt(len)];
                } while (pk.getPrimaryKey());
                table.primaryKeys[i] = pk;
                pk.setPrimaryKey(true);
                pk.setNullable(false);
            }
        }
        return table;
    }

    /**
     * Create a new random index.
     *
     * @return the index
     */
    Index newRandomIndex() {
        String indexName = "I_" + config.randomIdentifier();
        int len = config.random().getLog(getColumnCount() - 1) + 1;
        boolean unique = config.random().getBoolean(50);
        Column[] cols = getRandomColumns(len);
        Index index = new Index(this, indexName, cols, unique);
        return index;
    }

    /**
     * Get the DROP TABLE statement for this table.
     *
     * @return the SQL statement
     */
    String getDropSQL() {
        return "DROP TABLE " + name;
    }

    /**
     * Get the CREATE TABLE statement for this table.
     *
     * @return the SQL statement
     */
    String getCreateSQL() {
        String sql = "CREATE ";
        if (temporary) {
            if (globalTemporary) {
                sql += "GLOBAL ";
            } else {
                sql += "LOCAL ";
            }
            sql += "TEMPORARY ";
        }
        sql += "TABLE " + name + "(";
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sql += ", ";
            }
            Column column = columns[i];
            sql += column.getCreateSQL();
            if (primaryKeys != null && primaryKeys.length == 1 &&
                    primaryKeys[0] == column) {
                sql += " PRIMARY KEY";
            }
        }
        if (primaryKeys != null && primaryKeys.length > 1) {
            sql += ", ";
            sql += "PRIMARY KEY(";
            for (int i = 0; i < primaryKeys.length; i++) {
                if (i > 0) {
                    sql += ", ";
                }
                Column column = primaryKeys[i];
                sql += column.getName();
            }
            sql += ")";
        }
        sql += ")";
        return sql;
    }

    /**
     * Get the INSERT statement for this table.
     *
     * @param c the column list
     * @param v the value list
     * @return the SQL statement
     */
    String getInsertSQL(Column[] c, Value[] v) {
        String sql = "INSERT INTO " + name;
        if (c != null) {
            sql += "(";
            for (int i = 0; i < c.length; i++) {
                if (i > 0) {
                    sql += ", ";
                }
                sql += c[i].getName();
            }
            sql += ")";
        }
        sql += " VALUES(";
        for (int i = 0; i < v.length; i++) {
            if (i > 0) {
                sql += ", ";
            }
            sql += v[i].getSQL();
        }
        sql += ")";
        return sql;
    }

    /**
     * Get the table name.
     *
     * @return the name
     */
    String getName() {
        return name;
    }

    /**
     * Get a random column that can be used in a condition.
     *
     * @return the column
     */
    Column getRandomConditionColumn() {
        ArrayList<Column> list = New.arrayList();
        for (Column col : columns) {
            if (Column.isConditionType(config, col.getType())) {
                list.add(col);
            }
        }
        if (list.size() == 0) {
            return null;
        }
        return list.get(config.random().getInt(list.size()));
    }

    Column getRandomColumn() {
        return columns[config.random().getInt(columns.length)];
    }

    int getColumnCount() {
        return columns.length;
    }

    /**
     * Get a random column of the specified type.
     *
     * @param type the type
     * @return the column or null if no such column was found
     */
    Column getRandomColumnOfType(int type) {
        ArrayList<Column> list = New.arrayList();
        for (Column col : columns) {
            if (col.getType() == type) {
                list.add(col);
            }
        }
        if (list.size() == 0) {
            return null;
        }
        return list.get(config.random().getInt(list.size()));
    }

    /**
     * Get a number of random column from this table.
     *
     * @param len the column count
     * @return the columns
     */
    Column[] getRandomColumns(int len) {
        int[] index = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            index[i] = i;
        }
        for (int i = 0; i < columns.length; i++) {
            int temp = index[i];
            int r = index[config.random().getInt(columns.length)];
            index[i] = index[r];
            index[r] = temp;
        }
        Column[] c = new Column[len];
        for (int i = 0; i < len; i++) {
            c[i] = columns[index[i]];
        }
        return c;
    }

    Column[] getColumns() {
        return columns;
    }

    /**
     * Add this index to the table.
     *
     * @param index the index to add
     */
    void addIndex(Index index) {
        indexes.add(index);
    }

    /**
     * Remove an index from the table.
     *
     * @param index the index to remove
     */
    void removeIndex(Index index) {
        indexes.remove(index);
    }
}
