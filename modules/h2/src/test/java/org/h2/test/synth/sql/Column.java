/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * A column of a table.
 */
class Column {

    private static final int[] TYPES = { Types.INTEGER, Types.VARCHAR,
            Types.DECIMAL, Types.DATE, Types.TIME, Types.TIMESTAMP,
            Types.BOOLEAN, Types.BINARY, Types.VARBINARY, Types.CLOB,
            Types.BLOB, Types.DOUBLE, Types.BIGINT, Types.TIMESTAMP, Types.BIT, };

    private TestSynth config;
    private String name;
    private int type;
    private int precision;
    private int scale;
    private boolean isNullable;
    private boolean isPrimaryKey;
    // TODO test isAutoincrement;

    private Column(TestSynth config) {
        this.config = config;
    }

    Column(ResultSetMetaData meta, int index) throws SQLException {
        name = meta.getColumnLabel(index);
        type = meta.getColumnType(index);
        switch (type) {
        case Types.DECIMAL:
            precision = meta.getPrecision(index);
            scale = meta.getScale(index);
            break;
        case Types.BLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.CLOB:
        case Types.LONGVARCHAR:
        case Types.DATE:
        case Types.TIME:
        case Types.INTEGER:
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.BIGINT:
        case Types.NUMERIC:
        case Types.TIMESTAMP:
        case Types.NULL:
        case Types.LONGVARBINARY:
        case Types.DOUBLE:
        case Types.REAL:
        case Types.OTHER:
        case Types.BIT:
        case Types.BOOLEAN:
            break;
        default:
            throw new AssertionError("type=" + type);
        }
    }

    /**
     * Check if this data type supports comparisons for this database.
     *
     * @param config the configuration
     * @param type the SQL type
     * @return true if the value can be used in conditions
     */
    static boolean isConditionType(TestSynth config, int type) {
        switch (config.getMode()) {
        case TestSynth.H2:
        case TestSynth.H2_MEM:
            return true;
        case TestSynth.MYSQL:
        case TestSynth.HSQLDB:
        case TestSynth.POSTGRESQL:
            switch (type) {
            case Types.INTEGER:
            case Types.VARCHAR:
            case Types.DECIMAL:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.DOUBLE:
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.BIT:
                return true;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.LONGVARCHAR:
            case Types.LONGVARBINARY:
                return false;
            default:
                throw new AssertionError("type=" + type);
            }
        default:
            throw new AssertionError("type=" + type);
        }
    }

    private String getTypeName() {
        switch (type) {
        case Types.INTEGER:
            return "INT";
        case Types.VARCHAR:
            return "VARCHAR(" + precision + ")";
        case Types.DECIMAL:
            return "NUMERIC(" + precision + ", " + scale + ")";
        case Types.DATE:
            return "DATE";
        case Types.TIME:
            return "TIME";
        case Types.TIMESTAMP:
            return "TIMESTAMP";
        case Types.BINARY:
        case Types.VARBINARY:
            if (config.is(TestSynth.POSTGRESQL)) {
                return "BYTEA";
            }
            return "BINARY(" + precision + ")";
        case Types.CLOB: {
            if (config.is(TestSynth.HSQLDB)) {
                return "LONGVARCHAR";
            } else if (config.is(TestSynth.POSTGRESQL)) {
                return "TEXT";
            }
            return "CLOB";
        }
        case Types.BLOB: {
            if (config.is(TestSynth.HSQLDB)) {
                return "LONGVARBINARY";
            }
            return "BLOB";
        }
        case Types.DOUBLE:
            if (config.is(TestSynth.POSTGRESQL)) {
                return "DOUBLE PRECISION";
            }
            return "DOUBLE";
        case Types.BIGINT:
            return "BIGINT";
        case Types.BOOLEAN:
        case Types.BIT:
            return "BOOLEAN";
        default:
            throw new AssertionError("type=" + type);
        }
    }

    String getCreateSQL() {
        String sql = name + " " + getTypeName();
        if (!isNullable) {
            sql += " NOT NULL";
        }
        return sql;
    }

    String getName() {
        return name;
    }

    Value getRandomValue() {
        return Value.getRandom(config, type, precision, scale, isNullable);
    }

//    Value getRandomValueNotNull() {
//        return Value.getRandom(config, type, precision, scale, false);
//    }

    /**
     * Generate a random column.
     *
     * @param config the configuration
     * @return the column
     */
    static Column getRandomColumn(TestSynth config) {
        Column column = new Column(config);
        column.name = "C_" + config.randomIdentifier();
        int randomType;
        while (true) {
            randomType = TYPES[config.random().getLog(TYPES.length)];
            if (config.is(TestSynth.POSTGRESQL)
                    && (randomType == Types.BINARY ||
                    randomType == Types.VARBINARY ||
                    randomType == Types.BLOB)) {
                continue;
            }
            break;
        }
        column.type = randomType;
        column.precision = config.random().getInt(20) + 2;
        column.scale = config.random().getInt(column.precision);
        column.isNullable = config.random().getBoolean(50);
        return column;
    }

    boolean getPrimaryKey() {
        return isPrimaryKey;
    }

    void setPrimaryKey(boolean b) {
        isPrimaryKey = b;
    }

    void setNullable(boolean b) {
        isNullable = b;
    }

    /**
     * The the column type.
     *
     * @return the type
     */
    int getType() {
        return type;
    }

}
