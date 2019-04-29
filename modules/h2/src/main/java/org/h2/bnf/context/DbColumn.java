/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf.context;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Keeps the meta data information of a column.
 * This class is used by the H2 Console.
 */
public class DbColumn {

    private final String name;

    private final String quotedName;

    private final String dataType;

    private final int position;

    private DbColumn(DbContents contents, ResultSet rs, boolean procedureColumn)
            throws SQLException {
        name = rs.getString("COLUMN_NAME");
        quotedName = contents.quoteIdentifier(name);
        position = rs.getInt("ORDINAL_POSITION");
        if (contents.isH2() && !procedureColumn) {
            dataType = rs.getString("COLUMN_TYPE");
            return;
        }
        String type = rs.getString("TYPE_NAME");
        // a procedures column size is identified by PRECISION, for table this
        // is COLUMN_SIZE
        String precisionColumnName, scaleColumnName;
        if (procedureColumn) {
            precisionColumnName = "PRECISION";
            scaleColumnName = "SCALE";
        } else {
            precisionColumnName = "COLUMN_SIZE";
            scaleColumnName = "DECIMAL_DIGITS";
        }
        int precision = rs.getInt(precisionColumnName);
        if (precision > 0 && !contents.isSQLite()) {
            int scale = rs.getInt(scaleColumnName);
            if (scale > 0) {
                type = type + '(' + precision + ", " + scale + ')';
            } else {
                type = type + '(' + precision + ')';
            }
        }
        if (rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls) {
            type += " NOT NULL";
        }
        dataType = type;
    }

    /**
     * Create a column from a DatabaseMetaData.getProcedureColumns row.
     *
     * @param contents the database contents
     * @param rs the result set
     * @return the column
     */
    public static DbColumn getProcedureColumn(DbContents contents, ResultSet rs)
            throws SQLException {
        return new DbColumn(contents, rs, true);
    }

    /**
     * Create a column from a DatabaseMetaData.getColumns row.
     *
     * @param contents the database contents
     * @param rs the result set
     * @return the column
     */
    public static DbColumn getColumn(DbContents contents, ResultSet rs)
            throws SQLException {
        return new DbColumn(contents, rs, false);
    }

    /**
     * @return The data type name (including precision and the NOT NULL flag if
     * applicable).
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * @return The column name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The quoted table name.
     */
    public String getQuotedName() {
        return quotedName;
    }

    /**
     * @return Column index
     */
    public int getPosition() {
        return position;
    }

}
