/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;

import org.h2.value.Transfer;

/**
 * A result set column of a remote result.
 */
public class ResultColumn {

    /**
     * The column alias.
     */
    final String alias;

    /**
     * The schema name or null.
     */
    final String schemaName;

    /**
     * The table name or null.
     */
    final String tableName;

    /**
     * The column name or null.
     */
    final String columnName;

    /**
     * The value type of this column.
     */
    final int columnType;

    /**
     * The precision.
     */
    final long precision;

    /**
     * The scale.
     */
    final int scale;

    /**
     * The expected display size.
     */
    final int displaySize;

    /**
     * True if this is an autoincrement column.
     */
    final boolean autoIncrement;

    /**
     * True if this column is nullable.
     */
    final int nullable;

    /**
     * Read an object from the given transfer object.
     *
     * @param in the object from where to read the data
     */
    ResultColumn(Transfer in) throws IOException {
        alias = in.readString();
        schemaName = in.readString();
        tableName = in.readString();
        columnName = in.readString();
        columnType = in.readInt();
        precision = in.readLong();
        scale = in.readInt();
        displaySize = in.readInt();
        autoIncrement = in.readBoolean();
        nullable = in.readInt();
    }

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     */
    public static void writeColumn(Transfer out, ResultInterface result, int i)
            throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }

}
